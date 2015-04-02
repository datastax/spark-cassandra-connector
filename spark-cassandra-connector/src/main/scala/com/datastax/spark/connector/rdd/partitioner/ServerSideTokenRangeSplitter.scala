package com.datastax.spark.connector.rdd.partitioner

import java.io.IOException
import java.net.InetAddress

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

import org.apache.cassandra.thrift.CfSplit
import org.apache.spark.Logging

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}

/** Delegates token range splitting to Cassandra server. */
class ServerSideTokenRangeSplitter[V, T <: Token[V]](
  connector: CassandraConnector,
  keyspaceName: String,
  tableName: String,
  tokenFactory: TokenFactory[V, T])
  extends TokenRangeSplitter[V, T] with Logging {

  private def unthriftify(cfSplit: CfSplit, endpoints: Set[InetAddress]): TokenRange[V, T] = {
    val left = tokenFactory.tokenFromString(cfSplit.start_token)
    val right = tokenFactory.tokenFromString(cfSplit.end_token)
    TokenRange(left, right, endpoints, Some(cfSplit.row_count))
  }

  private def fetchSplits(
      range: TokenRange[V, T],
      endpoint: InetAddress,
      splitSize: Long): Seq[TokenRange[V, T]] = {

    val startToken = tokenFactory.tokenToString(range.start)
    val endToken = tokenFactory.tokenToString(range.end)

    connector.withCassandraClientDo(endpoint) {
      client =>
        client.set_keyspace(keyspaceName)
        client
          .describe_splits_ex(tableName, startToken, endToken, splitSize.toInt)
          .map(unthriftify(_, range.replicas))
    }
  }

  def split(range: TokenRange[V, T], splitSize: Long): Seq[TokenRange[V, T]] = {
    val fetchResults =
      for (replica <- range.replicas.toStream)
      yield Try(fetchSplits(range, replica, splitSize))

    fetchResults
      .collectFirst { case Success(splits) => splits }
      .getOrElse {
        for (Failure(e) <- fetchResults)
          logError("Failure while fetching splits from Cassandra", e)
        if (range.replicas.isEmpty)
          throw new IOException(
            s"Failed to fetch splits of $range " +
              s"because there are no replicas for the keyspace in the current datacenter.")
        else
          throw new IOException(
            s"Failed to fetch splits of $range from all endpoints: ${range.replicas.mkString(", ")}")
      }
  }
}