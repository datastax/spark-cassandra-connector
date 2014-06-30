package com.datastax.driver.spark.rdd.partitioner

import java.io.IOException
import java.net.InetAddress

import com.datastax.driver.spark.connector.CassandraConnector
import com.datastax.driver.spark.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}
import org.apache.cassandra.thrift.CfSplit
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/** Delegates token range splitting to Cassandra server. */
class ServerSideTokenRangeSplitter[V, T <: Token[V]](
  connector: CassandraConnector,
  keyspaceName: String,
  tableName: String,
  tokenFactory: TokenFactory[V, T])
  extends TokenRangeSplitter[V, T] with Logging {

  private def unthriftify(cfSplit: CfSplit, endpoints: Set[InetAddress]): TokenRange[V, T] = {
    val left = tokenFactory.fromString(cfSplit.start_token)
    val right = tokenFactory.fromString(cfSplit.end_token)
    TokenRange(left, right, endpoints, Some(cfSplit.row_count))
  }

  private def fetchSplits(range: TokenRange[V, T], endpoint: InetAddress, splitSize: Long): Seq[TokenRange[V, T]] = {
    val startToken = tokenFactory.toString(range.start)
    val endToken = tokenFactory.toString(range.end)

    connector.withCassandraClientDo(endpoint) {
      client =>
        client.set_keyspace(keyspaceName)
        client
          .describe_splits_ex(tableName, startToken, endToken, splitSize.toInt)
          .map(unthriftify(_, range.endpoints))
    }
  }

  def split(range: TokenRange[V, T], splitSize: Long) = {
    val fetchResults =
      for (endpoint <- range.endpoints.toStream)
      yield Try(fetchSplits(range, endpoint, splitSize))

    fetchResults
      .collectFirst { case Success(splits) => splits }
      .getOrElse {
        for (Failure(e) <- fetchResults)
          logError("Failure while fetching splits from Cassandra", e)
        throw new IOException(
          "Failed to fetch splits of %s from all endpoints: %s"
          .format(range, range.endpoints.mkString(", ")))
      }
  }
}