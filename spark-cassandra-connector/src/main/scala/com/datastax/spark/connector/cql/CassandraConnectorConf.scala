package com.datastax.spark.connector.cql

import java.net.InetAddress

import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkConf}

import com.datastax.driver.core.{ProtocolOptions, SSLOptions}
import com.datastax.spark.connector.util.ConfigCheck

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  hosts: Set[InetAddress],
  port: Int = CassandraConnectorConf.DefaultPort,
  authConf: AuthConf = NoAuthConf,
  localDC: Option[String] = None,
  keepAliveMillis: Int = CassandraConnectorConf.DefaultKeepAliveMillis,
  minReconnectionDelayMillis: Int = CassandraConnectorConf.DefaultMinReconnectionDelayMillis,
  maxReconnectionDelayMillis: Int = CassandraConnectorConf.DefaultMaxReconnectionDelayMillis,
  compression: ProtocolOptions.Compression = CassandraConnectorConf.DefaultCassandraConnectionCompression,
  queryRetryCount: Int = CassandraConnectorConf.DefaultQueryRetryCount,
  connectTimeoutMillis: Int = CassandraConnectorConf.DefaultConnectTimeoutMillis,
  readTimeoutMillis: Int = CassandraConnectorConf.DefaultReadTimeoutMillis,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
  cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf
)

/** A factory for [[CassandraConnectorConf]] objects.
  * Allows for manually setting connection properties or reading them from [[org.apache.spark.SparkConf SparkConf]]
  * object. By embedding connection information in [[org.apache.spark.SparkConf SparkConf]],
  * [[org.apache.spark.SparkContext SparkContext]] can offer Cassandra specific methods which require establishing
  * connections to a Cassandra cluster. */
object CassandraConnectorConf extends Logging {

  case class CassandraSSLConf(
    enabled: Boolean = false,
    trustStorePath: Option[String] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: String = "JKS",
    protocol: String = "TLS",
    enabledAlgorithms: Array[String] = SSLOptions.DEFAULT_SSL_CIPHER_SUITES
  )

  val DefaultPort = 9042

  val DefaultKeepAliveMillis = 250
  val DefaultMinReconnectionDelayMillis = 1000
  val DefaultMaxReconnectionDelayMillis = 60000
  val DefaultQueryRetryCount = 10
  val DefaultConnectTimeoutMillis = 5000
  val DefaultReadTimeoutMillis = 12000
  val DefaultCassandraConnectionCompression = ProtocolOptions.Compression.NONE

  val DefaultCassandraSSLConf = CassandraSSLConf()

  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionPortProperty = "spark.cassandra.connection.port"

  val CassandraConnectionLocalDCProperty = "spark.cassandra.connection.local_dc"
  val CassandraConnectionTimeoutProperty = "spark.cassandra.connection.timeout_ms"
  val CassandraConnectionKeepAliveProperty = "spark.cassandra.connection.keep_alive_ms"
  val CassandraMinReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.min"
  val CassandraMaxReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.max"
  val CassandraConnectionCompressionProperty = "spark.cassandra.connection.compression"
  val CassandraQueryRetryCountProperty = "spark.cassandra.query.retry.count"
  val CassandraReadTimeoutProperty = "spark.cassandra.read.timeout_ms"

  val CassandraConnectionSSLEnabledProperty = "spark.cassandra.connection.ssl.enabled"
  val CassandraConnectionSSLTrustStorePathProperty = "spark.cassandra.connection.ssl.trustStore.path"
  val CassandraConnectionSSLTrustStorePasswordProperty = "spark.cassandra.connection.ssl.trustStore.password"
  val CassandraConnectionSSLTrustStoreTypeProperty = "spark.cassandra.connection.ssl.trustStore.type"
  val CassandraConnectionSSLProtocolProperty = "spark.cassandra.connection.ssl.protocol"
  val CassandraConnectionSSLEnabledAlgorithmsProperty = "spark.cassandra.connection.ssl.enabledAlgorithms"

  //Whitelist for allowed CassandraConnector environment variables
  val Properties = Set(
    CassandraConnectionHostProperty,
    CassandraConnectionPortProperty,
    CassandraConnectionLocalDCProperty,
    CassandraConnectionTimeoutProperty,
    CassandraConnectionKeepAliveProperty,
    CassandraMinReconnectionDelayProperty,
    CassandraMaxReconnectionDelayProperty,
    CassandraConnectionCompressionProperty,
    CassandraQueryRetryCountProperty,
    CassandraReadTimeoutProperty,
    CassandraConnectionSSLEnabledProperty,
    CassandraConnectionSSLTrustStorePathProperty,
    CassandraConnectionSSLTrustStorePasswordProperty,
    CassandraConnectionSSLTrustStoreTypeProperty,
    CassandraConnectionSSLProtocolProperty,
    CassandraConnectionSSLEnabledAlgorithmsProperty
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
      hostAddress <- resolveHost(hostName.trim)
    } yield hostAddress
    
    val port = conf.getInt(CassandraConnectionPortProperty, DefaultPort)
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

    val sslEnabled = conf.getBoolean(CassandraConnectionSSLEnabledProperty,
      defaultValue = DefaultCassandraSSLConf.enabled)
    val sslTrustStorePath = conf.getOption(CassandraConnectionSSLTrustStorePathProperty)
    val sslTrustStorePassword = conf.getOption(CassandraConnectionSSLTrustStorePasswordProperty)
    val sslTrustStoreType = conf.get(CassandraConnectionSSLTrustStoreTypeProperty,
      defaultValue = DefaultCassandraSSLConf.trustStoreType)
    val sslProtocol = conf.get(CassandraConnectionSSLProtocolProperty,
      defaultValue = DefaultCassandraSSLConf.protocol)
    val sslEnabledAlgorithms = conf.getOption(CassandraConnectionSSLEnabledAlgorithmsProperty)
      .map(_.split(",").map(_.trim)).getOrElse(DefaultCassandraSSLConf.enabledAlgorithms)

    val cassandraSSLConf = CassandraSSLConf(
      enabled = sslEnabled,
      trustStorePath = sslTrustStorePath,
      trustStorePassword = sslTrustStorePassword,
      trustStoreType = sslTrustStoreType,
      protocol = sslProtocol,
      enabledAlgorithms = sslEnabledAlgorithms
    )

    CassandraConnectorConf(
      hosts = hosts,
      port = port,
      authConf = authConf,
      localDC = localDC,
      keepAliveMillis = keepAlive,
      minReconnectionDelayMillis = minReconnectionDelay,
      maxReconnectionDelayMillis = maxReconnectionDelay,
      compression = compression,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory,
      cassandraSSLConf = cassandraSSLConf
    )
  }
}
