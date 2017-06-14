package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Try

import com.datastax.spark.connector.util.Logging

import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.writer.RowWriterFactory

/** Creates CassandraPartitions for given Cassandra table */
private[connector] class CassandraPartitionGenerator[V, T <: Token[V]](
    connector: CassandraConnector,
    tableDef: TableDef,
    splitCount: Int)(
  implicit
    tokenFactory: TokenFactory[V, T]) extends Logging{

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName

  private def tokenRange(range: DriverTokenRange, metadata: Metadata): TokenRange = {
    val startToken = tokenFactory.tokenFromString(range.getStart.getValue.toString)
    val endToken = tokenFactory.tokenFromString(range.getEnd.getValue.toString)
    val replicas = metadata.getReplicas(Metadata.quote(keyspaceName), range).map(_.getAddress).toSet
    new TokenRange(startToken, endToken, replicas, tokenFactory)
  }

  private def describeRing: Seq[TokenRange] = {
    val ranges = connector.withClusterDo { cluster =>
      val metadata = cluster.getMetadata
      for (tr <- metadata.getTokenRanges.toSeq) yield tokenRange(tr, metadata)
    }

    /**
      * When we have a single Spark Partition use a single global range. This
      * will let us more easily deal with Partition Key equals and In clauses
      */
    if (splitCount == 1) {
      Seq(ranges.head.copy[V, T](tokenFactory.minToken, tokenFactory.minToken))
    } else {
      ranges
    }
  }

  private def createTokenRangeSplitter: TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        new RandomPartitionerTokenRangeSplitter().asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        new Murmur3PartitionerTokenRangeSplitter().asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported TokenFactory $tokenFactory")
    }
  }

  private def rangeToCql(range: TokenRange): Seq[CqlTokenRange[V, T]] =
    range.unwrap.map(CqlTokenRange(_))

  def partitions: Seq[CassandraPartition[V, T]] = {
    val tokenRanges = describeRing
    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val maxGroupSize = tokenRanges.size / endpointCount

    val splitter = createTokenRangeSplitter
    val splits = splitter.split(tokenRanges, splitCount).toSeq

    val clusterer = new TokenRangeClusterer[V, T](splitCount, maxGroupSize)
    val tokenRangeGroups = clusterer.group(splits).toArray

    val partitions = for (group <- tokenRangeGroups) yield {
      val replicas = group.map(_.replicas).reduce(_ intersect _)
      val rowCount = group.map(_.rangeSize).sum
      val cqlRanges = group.flatMap(rangeToCql)
      // partition index will be set later
      CassandraPartition(0, replicas, cqlRanges, rowCount.toLong)
    }

    // sort partitions and assign sequential numbers so that
    // partition index matches the order of partitions in the sequence
    partitions
      .sortBy(p => (p.endpoints.size, -p.dataSize))
      .zipWithIndex
      .map { case (p, index) => p.copy(index = index) }
  }

  /**
    * Attempts to build a partitioner for this C* RDD if it was keyed with Type Key. If possible
    * returns a partitioner of type Key. The type is required so we know what kind of objects we
    * will need to bind to prepared statements when determining the token on new objects.
    */
  def partitioner[Key: ClassTag : RowWriterFactory](
      keyMapper: ColumnSelector): Option[CassandraPartitioner[Key, V, T]] = {

    val part = Try {
      val newPartitioner = new CassandraPartitioner(connector, tableDef, partitions, keyMapper)
      // This is guaranteed to succeed so we don't want to send out an ERROR message if it breaks
      newPartitioner.verify(log = false)
      newPartitioner
    }

    if (part.isFailure) {
      logDebug(s"Not able to automatically create a partitioner: ${part.failed.get.getMessage}")
    }

    part.toOption
  }
}

object CassandraPartitionGenerator {

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  /** Creates a `CassandraPartitionGenerator` for the given cluster and table.
    * Unlike the class constructor, this method does not take the generic `V` and `T` parameters,
    * and therefore you don't need to specify the ones proper for the partitioner used in the
    * Cassandra cluster. */
  def apply(
    conn: CassandraConnector,
    tableDef: TableDef,
    splitCount: Int)(
    implicit tokenFactory: TokenFactory[V, T]): CassandraPartitionGenerator[V, T] = {

    new CassandraPartitionGenerator(conn, tableDef, splitCount)(tokenFactory)
  }
}
