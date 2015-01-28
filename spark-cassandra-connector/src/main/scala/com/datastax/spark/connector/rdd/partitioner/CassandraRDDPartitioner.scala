package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.rdd.partitioner.dht.{CassandraNode, Token, TokenFactory}
import com.datastax.spark.connector.util.CqlWhereParser
import com.datastax.spark.connector.util.CqlWhereParser._
import org.apache.cassandra.thrift
import org.apache.cassandra.thrift.Cassandra
import org.apache.spark.Partition
import org.apache.thrift.TApplicationException

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

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

  private val NullAddress = InetAddress.getByName("0.0.0.0")

  /* An incorrectly configured Cassandra node may send us 0.0.0.0 as the rpc_address.
   * If this happens, we replace it with the local address (which must be always != 0.0.0.0) */
  private def cassandraNode(rpcAddress: InetAddress, localAddress: InetAddress): CassandraNode = {
    (rpcAddress, localAddress) match {
      case (NullAddress, NullAddress) => throw new IllegalArgumentException(
        "Broadcast address and RPC address of a Cassandra node cannot be both set to 0.0.0.0")
      case (NullAddress, local) => CassandraNode(local, local)
      case (rpc, NullAddress) => CassandraNode(rpc, rpc)
      case (rpc, local) => CassandraNode(rpc, local)
    }
  }

  private def unthriftify(tr: thrift.TokenRange): TokenRange = {
    val startToken = tokenFactory.fromString(tr.start_token)
    val endToken = tokenFactory.fromString(tr.end_token)
    val rpcAddresses = tr.rpc_endpoints.map(InetAddress.getByName)
    val localAddresses = tr.endpoints.map(InetAddress.getByName)
    val endpoints = (rpcAddresses zip localAddresses).map(Function.tupled(cassandraNode)).toSet
    new TokenRange(startToken, endToken, endpoints, None)
  }

  private def describeRing(client: Cassandra.Iface): Seq[TokenRange] = {
    val ring =
      try {
        client.describe_local_ring(keyspaceName)
      }
      catch {
        case e: TApplicationException if e.getType == TApplicationException.UNKNOWN_METHOD =>
          client.describe_ring(keyspaceName)
        case e: java.lang.NoSuchMethodError =>
          client.describe_ring(keyspaceName)
      }
    ring.map(unthriftify)
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
        val endpointCount = tokenRanges.map(_.endpoints).reduce(_ ++ _).size
        val splitter = createSplitterFor(tokenRanges)
        val splits = splitsOf(tokenRanges, splitter).toSeq
        val maxGroupSize = tokenRanges.size / endpointCount
        val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
        val groups = clusterer.group(splits).toArray

        if (containsPartitionKey(whereClause)) {
          val endpoints = tokenRanges.flatMap(_.endpoints)
          val addresses = endpoints.flatMap(_.allAddresses)
          Array(CassandraPartition(0, addresses, List(CqlTokenRange("")), 0))
        }
        else
          for ((group, index) <- groups.zipWithIndex) yield {
            val cqlPredicates = group.flatMap(splitToCqlClause)
            val endpoints = group.map(_.endpoints).reduce(_ intersect _)
            val rowCount = group.map(_.rowCount.get).sum
            CassandraPartition(index, endpoints.flatMap(_.allAddresses), cqlPredicates, rowCount)
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
