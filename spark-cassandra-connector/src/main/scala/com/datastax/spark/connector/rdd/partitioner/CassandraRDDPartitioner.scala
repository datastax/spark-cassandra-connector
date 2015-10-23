package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

import org.apache.spark.Partition

import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.util.CqlWhereParser
import com.datastax.spark.connector.util.CqlWhereParser._
import com.datastax.spark.connector.util.Quote._

/** Creates CassandraPartitions for given Cassandra table */
class CassandraRDDPartitioner[V, T <: Token[V]](
    connector: CassandraConnector,
    tableDef: TableDef,
    splitCount: Option[Int],
    splitSize: Long)(
  implicit
    tokenFactory: TokenFactory[V, T]) {

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName

  private val totalDataSize: Long = {
    // If we know both the splitCount and splitSize, we should pretend the total size of the data is
    // their multiplication. TokenRangeSplitter will try to produce splits of desired size, and this way
    // their number will be close to desired splitCount. Otherwise, if splitCount is not set,
    // we just go to C* and read the estimated data size from an appropriate system table
    splitCount match {
      case Some(c) => c * splitSize
      case None => new DataSizeEstimates(connector, keyspaceName, tableName).dataSizeInBytes
    }
  }

  def tokenRange(range: DriverTokenRange, metadata: Metadata): TokenRange = {
    val startToken = tokenFactory.tokenFromString(range.getStart.getValue.toString)
    val endToken = tokenFactory.tokenFromString(range.getEnd.getValue.toString)
    val replicas = metadata.getReplicas(Metadata.quote(keyspaceName), range).map(_.getAddress).toSet
    val dataSize = (tokenFactory.ringFraction(startToken, endToken) * totalDataSize).toLong
    new TokenRange(startToken, endToken, replicas, dataSize)
  }

  private def describeRing: Seq[TokenRange] = {
    connector.withClusterDo { cluster =>
      val metadata = cluster.getMetadata
      for (tr <- metadata.getTokenRanges.toSeq) yield tokenRange(tr, metadata)
    }
  }

  private def splitsOf(
      tokenRanges: Iterable[TokenRange],
      splitter: TokenRangeSplitter[V, T]): Iterable[TokenRange] = {

    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraRDDPartitioner.pool)
    (for (tokenRange <- parTokenRanges;
          split <- splitter.split(tokenRange, splitSize)) yield split).seq
  }

  private def splitToCqlClause(range: TokenRange): Iterable[CqlTokenRange] = {
    val startToken = range.start.value
    val endToken = range.end.value
    val pk = tableDef.partitionKey.map(_.columnName).map(quote).mkString(", ")

    if (range.end == tokenFactory.minToken)
      List(CqlTokenRange(s"token($pk) > ?", startToken))
    else if (range.start == tokenFactory.minToken)
      List(CqlTokenRange(s"token($pk) <= ?", endToken))
    else if (!range.isWrapAround)
      List(CqlTokenRange(s"token($pk) > ? AND token($pk) <= ?", startToken, endToken))
    else
      List(
        CqlTokenRange(s"token($pk) > ?", startToken),
        CqlTokenRange(s"token($pk) <= ?", endToken))
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

  private def containsPartitionKey(clause: CqlWhereClause) = {
    val pk = tableDef.partitionKey.map(_.columnName).toSet
    val wherePredicates: Seq[Predicate] = clause.predicates.flatMap(CqlWhereParser.parse)

    val whereColumns: Set[String] = wherePredicates.collect {
      case EqPredicate(c, _) if pk.contains(c) => c
      case InPredicate(c) if pk.contains(c) => c
      case InListPredicate(c, _) if pk.contains(c) => c
      case RangePredicate(c, _, _) if pk.contains(c) =>
        throw new UnsupportedOperationException(
          s"Range predicates on partition key columns (here: $c) are " +
            s"not supported in where. Use filter instead.")
    }.toSet

    if (whereColumns.nonEmpty && whereColumns.size < pk.size) {
      val missing = pk -- whereColumns
      throw new UnsupportedOperationException(
        s"Partition key predicate must include all partition key columns. Missing columns: ${missing.mkString(",")}"
      )
    }

    whereColumns.nonEmpty
  }

  /** Computes Spark partitions of the given table. Called by [[CassandraTableScanRDD]]. */
  def partitions(whereClause: CqlWhereClause): Array[Partition] = {
    val tokenRanges = describeRing
    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val splitter = createTokenRangeSplitter
    val splits = splitsOf(tokenRanges, splitter).toSeq
    val maxGroupSize = tokenRanges.size / endpointCount
    val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
    val groups = clusterer.group(splits).toArray

    if (containsPartitionKey(whereClause)) {
      val replicas = tokenRanges.flatMap(_.replicas)
      Array(CassandraPartition(0, replicas, List(CqlTokenRange("")), 0))
    }
    else
      (for ((group, index) <- groups.zipWithIndex) yield {
        val cqlPredicates = group.flatMap(splitToCqlClause)
        val replicas = group.map(_.replicas).reduce(_ intersect _)
        val rowCount = group.map(_.dataSize).sum
        CassandraPartition(index, replicas, cqlPredicates, rowCount)
      })
      // sort groups to give scheduler more flexibility at the end of calculations
      // tasks with more endpoints will be executed latter
      .sortBy(p=>(p.endpoints.size, -p.rowCount)).toArray
  }

}

object CassandraRDDPartitioner {
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token ranges to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  /** Creates a `CassandraRDDPartitioner` for the given cluster and table.
    * Unlike the class constructor, this method does not take the generic `V` and `T` parameters,
    * and therefore you don't need to specify the ones proper for the partitioner used in the
    * Cassandra cluster. */
  def apply(
      conn: CassandraConnector,
      tableDef: TableDef,
      splitCount: Option[Int],
      splitSize: Int): CassandraRDDPartitioner[V, T] = {

    val tokenFactory = getTokenFactory(conn)
    new CassandraRDDPartitioner(conn, tableDef, splitCount, splitSize)(tokenFactory)
  }

  def getTokenFactory(conn: CassandraConnector) : TokenFactory[V, T] = {
    val partitionerName = conn.withSessionDo { session =>
      session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }
    TokenFactory.forCassandraPartitioner(partitionerName)
  }
}
