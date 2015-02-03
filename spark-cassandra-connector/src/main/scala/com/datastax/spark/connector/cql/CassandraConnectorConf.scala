package com.datastax.spark.connector.cql

import java.net.InetAddress
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import com.datastax.spark.connector.util.Logging

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  hosts: Set[InetAddress],
  nativePort: Int = CassandraConnectorConf.DefaultNativePort,
  rpcPort: Int = CassandraConnectorConf.DefaultRpcPort,
  authConf: AuthConf = NoAuthConf,
  keepAliveMillis: Int = CassandraConnectorConf.DefaultKeepAliveMillis,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory
)

/** A factory for `CassandraConnectorConf` objects.
  * Allows for manually setting connection properties or reading them from `SparkConf` object.
  * By embedding connection information in `SparkConf`, `SparkContext` can offer Cassandra specific methods
  * which require establishing connections to a Cassandra cluster.*/
object CassandraConnectorConf extends Logging {

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042
  val DefaultKeepAliveMillis = 250

  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "spark.cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "spark.cassandra.connection.native.port"
  val CassandraConnectionKeepAliveProperty = "spark.cassandra.connection.keep_alive_ms"

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
    val keepAlive = conf.getInt(CassandraConnectionKeepAliveProperty, DefaultKeepAliveMillis)
    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)
    CassandraConnectorConf(hosts, nativePort, rpcPort, authConf, keepAlive, connectionFactory)
  }
}
