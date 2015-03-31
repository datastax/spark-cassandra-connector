package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

import org.apache.cassandra.thrift.Cassandra
import org.apache.spark.Partition

import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.util.CqlWhereParser
import com.datastax.spark.connector.util.CqlWhereParser._

/** Creates CassandraPartitions for given Cassandra table */
class CassandraRDDPartitioner[V, T <: Token[V]](
    connector: CassandraConnector,
    tableDef: TableDef,
    splitSize: Long)(
  implicit
    tokenFactory: TokenFactory[V, T]) {

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName

  def tokenRange(range: DriverTokenRange, metadata: Metadata): TokenRange = {
    val startToken = tokenFactory.fromString(range.getStart.getValue.toString)
    val endToken = tokenFactory.fromString(range.getEnd.getValue.toString)
    val replicas = metadata.getReplicas(keyspaceName, range).map(_.getAddress).toSet
    new TokenRange(startToken, endToken, replicas, None)
  }

  private def describeRing(client: Cassandra.Iface): Seq[TokenRange] = {
    connector.withClusterDo { cluster =>
      val metadata = cluster.getMetadata
      for (tr <- metadata.getTokenRanges.toSeq) yield tokenRange(tr, metadata)
    }
  }

  private def quote(name: String) = "\"" + name + "\""

  private def splitsOf(tokenRanges: Iterable[TokenRange], splitter: TokenRangeSplitter[V, T]): Iterable[TokenRange] = {
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

  /** This works only for numeric tokens */
  private def tokenCount(range: TokenRange): BigInt = {
    val start = BigInt(tokenFactory.toString(range.start))
    val end = BigInt(tokenFactory.toString(range.end))
    if (start < end)
      end - start
    else
      end - start + tokenFactory.totalTokenCount
  }

  /** Rows per token average is required for fast local range splitting.
    * Used only for Murmur3Partitioner and RandomPartitioner.  */
  private def estimateCassandraPartitionsPerToken(tokenRanges: Seq[TokenRange]): Double = {
    val random = new scala.util.Random(0)
    val tokenRangeSample = random.shuffle(tokenRanges).take(CassandraRDDPartitioner.TokenRangeSampleSize)
    val splitter = new ServerSideTokenRangeSplitter(connector, keyspaceName, tableName, tokenFactory)
    val splits = splitsOf(tokenRangeSample, splitter)
    val tokenCountSum = splits.map(tokenCount).sum
    val rowCountSum = splits.map(_.rowCount.get).sum
    rowCountSum.toDouble / tokenCountSum.toDouble
  }

  private def createSplitterFor(tokenRanges: Seq[TokenRange]): TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        val partitionsPerToken = estimateCassandraPartitionsPerToken(tokenRanges)
        new RandomPartitionerTokenRangeSplitter(partitionsPerToken).asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        val partitionsPerToken = estimateCassandraPartitionsPerToken(tokenRanges)
        new Murmur3PartitionerTokenRangeSplitter(partitionsPerToken).asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        new ServerSideTokenRangeSplitter(connector, keyspaceName, tableName, tokenFactory)
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
    connector.withCassandraClientDo {
      client =>
        val tokenRanges = describeRing(client)
        val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
        val splitter = createSplitterFor(tokenRanges)
        val splits = splitsOf(tokenRanges, splitter).toSeq
        val maxGroupSize = tokenRanges.size / endpointCount
        val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
        val groups = clusterer.group(splits).toArray

        if (containsPartitionKey(whereClause)) {
          val replicas = tokenRanges.flatMap(_.replicas)
          Array(CassandraPartition(0, replicas, List(CqlTokenRange("")), 0))
        }
        else
          for ((group, index) <- groups.zipWithIndex) yield {
            val cqlPredicates = group.flatMap(splitToCqlClause)
            val replicas = group.map(_.replicas).reduce(_ intersect _)
            val rowCount = group.map(_.rowCount.get).sum
            CassandraPartition(index, replicas, cqlPredicates, rowCount)
          }
    }
  }

}

object CassandraRDDPartitioner {
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token ranges to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)
}
