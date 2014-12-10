package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkConf
import scala.util.control.NonFatal

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(hosts: Set[InetAddress],
                                  nativePort: Int = CassandraConnectorConf.DefaultNativePort,
                                  rpcPort: Int = CassandraConnectorConf.DefaultRpcPort,
                                  authConf: AuthConf = NoAuthConf,
                                  connectionOptions: ConnectionOptions = ConnectionOptions(),
                                  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory)

/** A factory for `CassandraConnectorConf` objects.
  * Allows for manually setting connection properties or reading them from `SparkConf` object.
  * By embedding connection information in `SparkConf`, `SparkContext` can offer Cassandra specific methods
  * which require establishing connections to a Cassandra cluster. */
object CassandraConnectorConf extends Logging {

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042

  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "spark.cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "spark.cassandra.connection.native.port"

  private def resolveHost(hostName: String): Option[InetAddress] = {
    try Some(InetAddress.getByName(hostName))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  def apply(conf: SparkConf): CassandraConnectorConf = {
    val hostsStr = conf.get(CassandraConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName)
    } yield hostAddress

    val rpcPort = conf.getInt(CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(CassandraConnectionNativePortProperty, DefaultNativePort)
    val authConf = AuthConf.fromSparkConf(conf)
    val connectionOptions = ConnectionOptions(conf)
    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)
    CassandraConnectorConf(hosts, nativePort, rpcPort, authConf, connectionOptions, connectionFactory)
  }
}

case class ConnectionOptions(minReconnectionDelay: Int = 1000,
                             maxReconnectionDelay: Int = 60000,
                             localDC: String = null,
                             retryCount: Int = 10,
                             connectTimeout: Int = 5000,
                             readTimeout: Int = 12000)

object ConnectionOptions {
  def apply(conf: SparkConf): ConnectionOptions = {
    val minReconnectionDelay = conf.getInt("spark.cassandra.connection.reconnection_delay_ms.min", 1000)
    val maxReconnectionDelay = conf.getInt("spark.cassandra.connection.reconnection_delay_ms.max", 60000)
    val localDC = conf.get("spark.cassandra.connection.local_dc", null)
    val retryCount = conf.getInt("spark.cassandra.query.retry.count", 10)
    val connectTimeout = conf.getInt("spark.cassandra.connection.timeout_ms", 5000)
    val readTimeout = conf.getInt("spark.cassandra.read.timeout_ms", 12000)

    ConnectionOptions(minReconnectionDelay, maxReconnectionDelay,
      localDC, retryCount, connectTimeout, readTimeout)
  }
}