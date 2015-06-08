package com.datastax.spark.connector.cql

import java.net.InetAddress

import org.apache.spark.{Logging, SparkConf}

import scala.util.control.NonFatal

import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.util.ConfigCheck

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
  compression: ProtocolOptions.Compression = CassandraConnectorConf.DefaultCassandraConnectionCompression,
  queryRetryCount: Int = CassandraConnectorConf.DefaultQueryRetryCount,
  connectTimeoutMillis: Int = CassandraConnectorConf.DefaultConnectTimeoutMillis,
  readTimeoutMillis: Int = CassandraConnectorConf.DefaultReadTimeoutMillis,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory
)

/** A factory for [[CassandraConnectorConf]] objects.
  * Allows for manually setting connection properties or reading them from [[org.apache.spark.SparkConf SparkConf]]
  * object. By embedding connection information in [[org.apache.spark.SparkConf SparkConf]],
  * [[org.apache.spark.SparkContext SparkContext]] can offer Cassandra specific methods which require establishing
  * connections to a Cassandra cluster.*/
object CassandraConnectorConf extends Logging {

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042

  val DefaultKeepAliveMillis = 250
  val DefaultMinReconnectionDelayMillis = 1000
  val DefaultMaxReconnectionDelayMillis = 60000
  val DefaultQueryRetryCount = 10
  val DefaultConnectTimeoutMillis = 5000
  val DefaultReadTimeoutMillis = 12000
  val DefaultCassandraConnectionCompression = ProtocolOptions.Compression.NONE

  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "spark.cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "spark.cassandra.connection.native.port"

  val CassandraConnectionLocalDCProperty = "spark.cassandra.connection.local_dc"
  val CassandraConnectionTimeoutProperty = "spark.cassandra.connection.timeout_ms"
  val CassandraConnectionKeepAliveProperty = "spark.cassandra.connection.keep_alive_ms"
  val CassandraMinReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.min"
  val CassandraMaxReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.max"
  val CassandraConnectionCompressionProperty = "spark.cassandra.connection.compression"
  val CassandraQueryRetryCountProperty = "spark.cassandra.query.retry.count"
  val CassandraReadTimeoutProperty = "spark.cassandra.read.timeout_ms"

  //Whitelist for allowed CassandraConnector environment variables
  val Properties = Set(
    CassandraConnectionHostProperty,
    CassandraConnectionRpcPortProperty,
    CassandraConnectionNativePortProperty,
    CassandraConnectionLocalDCProperty,
    CassandraConnectionTimeoutProperty,
    CassandraConnectionKeepAliveProperty,
    CassandraMinReconnectionDelayProperty,
    CassandraMaxReconnectionDelayProperty,
    CassandraConnectionCompressionProperty,
    CassandraQueryRetryCountProperty,
    CassandraReadTimeoutProperty
  )
  
  private def resolveHost(hostName: String): Option[InetAddress] = {
    try Some(InetAddress.getByName(hostName))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  def apply(conf: SparkConf): CassandraConnectorConf = {
    ConfigCheck.checkConfig(conf)
    val hostsStr = conf.get(CassandraConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName)
    } yield hostAddress

    val rpcPort = conf.getInt(CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(CassandraConnectionNativePortProperty, DefaultNativePort)
    val authConf = AuthConf.fromSparkConf(conf)
    val keepAlive = conf.getInt(CassandraConnectionKeepAliveProperty, DefaultKeepAliveMillis)
    
    val localDC = conf.getOption(CassandraConnectionLocalDCProperty)
    val minReconnectionDelay = conf.getInt(CassandraMinReconnectionDelayProperty, DefaultMinReconnectionDelayMillis)
    val maxReconnectionDelay = conf.getInt(CassandraMaxReconnectionDelayProperty, DefaultMaxReconnectionDelayMillis)
    val queryRetryCount = conf.getInt(CassandraQueryRetryCountProperty, DefaultQueryRetryCount)
    val connectTimeout = conf.getInt(CassandraConnectionTimeoutProperty, DefaultConnectTimeoutMillis)
    val readTimeout = conf.getInt(CassandraReadTimeoutProperty, DefaultReadTimeoutMillis)

    val compression = conf.getOption(CassandraConnectionCompressionProperty)
        .map(ProtocolOptions.Compression.valueOf).getOrElse(DefaultCassandraConnectionCompression)

    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)

    CassandraConnectorConf(
      hosts = hosts,
      nativePort = nativePort,
      rpcPort = rpcPort,
      authConf = authConf,
      localDC = localDC,
      keepAliveMillis = keepAlive,
      minReconnectionDelayMillis = minReconnectionDelay,
      maxReconnectionDelayMillis = maxReconnectionDelay,
      compression = compression,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory)
  }
}
