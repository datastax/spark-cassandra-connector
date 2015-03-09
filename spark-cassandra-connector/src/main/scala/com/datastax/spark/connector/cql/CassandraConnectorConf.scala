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
  localDC: Option[String] = None,
  keepAliveMillis: Int = CassandraConnectorConf.DefaultKeepAliveMillis,
  minReconnectionDelayMillis: Int = CassandraConnectorConf.DefaultMinReconnectionDelayMillis,
  maxReconnectionDelayMillis: Int = CassandraConnectorConf.DefaultMaxReconnectionDelayMillis,
  queryRetryCount: Int = CassandraConnectorConf.DefaultQueryRetryCount,
  connectTimeoutMillis: Int = CassandraConnectorConf.DefaultConnectTimeoutMillis,
  readTimeoutMillis: Int = CassandraConnectorConf.DefaultReadTimeoutMillis,
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
  val DefaultMinReconnectionDelayMillis = 1000
  val DefaultMaxReconnectionDelayMillis = 60000
  val DefaultQueryRetryCount = 10
  val DefaultConnectTimeoutMillis = 5000
  val DefaultReadTimeoutMillis = 12000

  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "spark.cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "spark.cassandra.connection.native.port"

  val CassandraConnectionLocalDCProperty = "spark.cassandra.connection.local_dc"
  val CassandraConnectionTimeoutProperty = "spark.cassandra.connection.timeout_ms"
  val CassandraConnectionKeepAliveProperty = "spark.cassandra.connection.keep_alive_ms"
  val CassandraMinReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.min"
  val CassandraMaxReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.max"
  val CassandraQueryRetryCountProperty = "spark.cassandra.query.retry.count"
  val CassandraReadTimeoutProperty = "spark.cassandra.read.timeout_ms" 
  
  private def resolveHost(hostName: String): Option[InetAddress] = {
    try Some(InetAddress.getByName(hostName))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  def processProperty(property: String, cluster: Option[String] = None): String = {
    cluster match {
      case Some(c) => property.replaceFirst("spark.cassandra", s"spark.$c.cassandra")
      case None => property
    }
  }

  def apply(conf: SparkConf): CassandraConnectorConf = getCassandraConnectorConf(conf, None)

  def apply(conf: SparkConf, cluster: Option[String]): CassandraConnectorConf = getCassandraConnectorConf(conf, cluster)

  def getCassandraConnectorConf(conf: SparkConf, cluster: Option[String]): CassandraConnectorConf = {
    val hostsStr = conf.get(processProperty(CassandraConnectionHostProperty, cluster), InetAddress.getLocalHost.getHostAddress)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName)
    } yield hostAddress

    val rpcPort = conf.getInt(processProperty(CassandraConnectionRpcPortProperty, cluster), DefaultRpcPort)
    val nativePort = conf.getInt(processProperty(CassandraConnectionNativePortProperty, cluster), DefaultNativePort)
    val authConf = AuthConf.fromSparkConf(conf, cluster)
    val keepAlive = conf.getInt(processProperty(CassandraConnectionKeepAliveProperty, cluster), DefaultKeepAliveMillis)
    
    val localDC = conf.getOption(processProperty(CassandraConnectionLocalDCProperty, cluster))
    val minReconnectionDelay = conf.getInt(processProperty(CassandraMinReconnectionDelayProperty, cluster), DefaultMinReconnectionDelayMillis)
    val maxReconnectionDelay = conf.getInt(processProperty(CassandraMaxReconnectionDelayProperty, cluster), DefaultMaxReconnectionDelayMillis)
    val queryRetryCount = conf.getInt(processProperty(CassandraQueryRetryCountProperty, cluster), DefaultQueryRetryCount)
    val connectTimeout = conf.getInt(processProperty(CassandraConnectionTimeoutProperty, cluster), DefaultConnectTimeoutMillis)
    val readTimeout = conf.getInt(processProperty(CassandraReadTimeoutProperty, cluster), DefaultReadTimeoutMillis)

    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf, cluster)

    CassandraConnectorConf(
      hosts = hosts,
      nativePort = nativePort,
      rpcPort = rpcPort,
      authConf = authConf,
      localDC = localDC,
      keepAliveMillis = keepAlive,
      minReconnectionDelayMillis = minReconnectionDelay,
      maxReconnectionDelayMillis = maxReconnectionDelay,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory)
  }
}