package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Try


import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.util.Logging

/** Creates CassandraPartitions for given Cassandra table */
private[connector] class CassandraPartitionGenerator[V, T <: Token[V]](
    connector: CassandraConnector,
    tableDef: TableDef,
    splitCount: Option[Int],
    splitSize: Long)(
  implicit
    tokenFactory: TokenFactory[V, T]) extends Logging{

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName

  if (splitSize == 0)
    throw new IllegalArgumentException(s"Split Size Cannot be 0, Currently set to $splitSize")

  private val totalDataSize: Long = {
    // If we know both the splitCount and splitSize, we should pretend the total size of the data is
    // their multiplication. TokenRangeSplitter will try to produce splits of desired size, and this way
    // their number will be close to desired splitCount. Otherwise, if splitCount is not set,
    // we just go to C* and read the estimated data size from an appropriate system table
    splitCount match {
      case Some(c) => c * splitSize
      case None => {
        val estimate = new DataSizeEstimates(connector, keyspaceName, tableName).dataSizeInBytes
        if (estimate == Long.MaxValue) {
          val meta = connector.withClusterDo(_.getMetadata)
          val local_dc = connector.closestLiveHost.getDatacenter
          val endpoints = meta.getAllHosts.count(host => host.getDatacenter == local_dc)
          val targetPartitionsAmount = (endpoints * 2 + 1)
          logWarning(
            s"""Size Estimates has overflowed and calculated that the data size is Infinite.
              |Falling back to $targetPartitionsAmount (2 * EndpointsInDC + 1) Split Count.
              |This is most likely occurring because you are reading size_estimates
              |from a DataCenter which has very small primary ranges. Explicitly set
              |the splitCount when reading to manually adjust this""".stripMargin)
          targetPartitionsAmount * splitSize
        } else {
          estimate
        }
      }
    }
  }

  private def tokenRange(range: DriverTokenRange, metadata: Metadata): TokenRange = {
    val startToken = tokenFactory.tokenFromString(range.getStart.getValue.toString)
    val endToken = tokenFactory.tokenFromString(range.getEnd.getValue.toString)
    val replicas = metadata.getReplicas(Metadata.quote(keyspaceName), range).map(_.getAddress).toSet
    val dataSize = (tokenFactory.ringFraction(startToken, endToken) * totalDataSize).toLong
    new TokenRange(startToken, endToken, replicas, dataSize)
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
    if (splitCount == Some(1)) {
      Seq(ranges(0).copy[V, T](tokenFactory.minToken, tokenFactory.maxToken))
    } else {
      ranges
    }
  }

  private def splitsOf(
      tokenRanges: Iterable[TokenRange],
      splitter: TokenRangeSplitter[V, T]): Iterable[TokenRange] = {

    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraPartitionGenerator.pool)
    (for (tokenRange <- parTokenRanges;
          split <- splitter.split(tokenRange, splitSize)) yield split).seq
  }

  private def createTokenRangeSplitter: TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        new RandomPartitionerTokenRangeSplitter(totalDataSize).asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        new Murmur3PartitionerTokenRangeSplitter(totalDataSize).asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported TokenFactory $tokenFactory")
    }
  }

  private def rangeToCql(range: TokenRange): Seq[CqlTokenRange[V, T]] =
    range.unwrap.map(CqlTokenRange(_))

  def partitions: Seq[CassandraPartition[V, T]] = {
    val tokenRanges = describeRing
    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val splitter = createTokenRangeSplitter
    val splits = splitsOf(tokenRanges, splitter).toSeq
    val maxGroupSize = tokenRanges.size / endpointCount
    val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
    val tokenRangeGroups = clusterer.group(splits).toArray

    val partitions = for (group <- tokenRangeGroups) yield {
      val replicas = group.map(_.replicas).reduce(_ intersect _)
      val rowCount = group.map(_.dataSize).sum
      val cqlRanges = group.flatMap(rangeToCql)
      // partition index will be set later
      CassandraPartition(0, replicas, cqlRanges, rowCount)
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
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token rangesContaining to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  /** Creates a `CassandraPartitionGenerator` for the given cluster and table.
    * Unlike the class constructor, this method does not take the generic `V` and `T` parameters,
    * and therefore you don't need to specify the ones proper for the partitioner used in the
    * Cassandra cluster. */
  def apply(
      conn: CassandraConnector,
      tableDef: TableDef,
      splitCount: Option[Int],
      splitSize: Long): CassandraPartitionGenerator[V, T] = {

    val tokenFactory = getTokenFactory(conn)
    new CassandraPartitionGenerator(conn, tableDef, splitCount, splitSize)(tokenFactory)
  }

  def getTokenFactory(conn: CassandraConnector) : TokenFactory[V, T] = {
    val partitionerName = conn.withSessionDo { session =>
      session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }
    TokenFactory.forCassandraPartitioner(partitionerName)
  }
}
