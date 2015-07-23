package com.datastax.spark.connector.cql

import java.net.InetAddress

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkConf}

import com.datastax.driver.core.{ProtocolOptions, SSLOptions}
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
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
  cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf,
  queryRetryDelay: CassandraConnectorConf.RetryDelayConf = CassandraConnectorConf.DefaultQueryRetryDelay
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

  trait RetryDelayConf {
    def forRetry(retryNumber: Int): Duration
  }

  object RetryDelayConf extends Serializable {

    case class ConstantDelay(delay: Duration) extends RetryDelayConf {
      require(delay.length >= 0, "Delay must not be negative")

      override def forRetry(nbRetry: Int) = delay
    }

    case class LinearDelay(initialDelay: Duration, increaseBy: Duration) extends RetryDelayConf {
      require(initialDelay.length >= 0, "Initial delay must not be negative")
      require(increaseBy.length > 0, "Delay increase must be greater than 0")

      override def forRetry(nbRetry: Int) = initialDelay + (increaseBy * (nbRetry - 1).max(0))
    }

    case class ExponentialDelay(initialDelay: Duration, increaseBy: Double) extends RetryDelayConf {
      require(initialDelay.length >= 0, "Initial delay must not be negative")
      require(increaseBy > 0, "Delay increase must be greater than 0")

      override def forRetry(nbRetry: Int) =
        (initialDelay.toMillis * math.pow(increaseBy, (nbRetry - 1).max(0))).toLong milliseconds
    }

    private val ConstantDelayEx = """(\d+)""".r
    private val LinearDelayEx = """(\d+)\+(.+)""".r
    private val ExponentialDelayEx = """(\d+)\*(.+)""".r

    def fromString(s: String): Option[RetryDelayConf] = s.trim match {
      case "" => None

      case ConstantDelayEx(delayStr) =>
        val d = for (delay <- Try(delayStr.toInt)) yield ConstantDelay(delay milliseconds)
        d.toOption.orElse(throw new IllegalArgumentException(
          s"Invalid format of constant delay: $s; it should be <integer number>."))

      case LinearDelayEx(delayStr, increaseStr) =>
        val d = for (delay <- Try(delayStr.toInt); increaseBy <- Try(increaseStr.toInt))
          yield LinearDelay(delay milliseconds, increaseBy milliseconds)
        d.toOption.orElse(throw new IllegalArgumentException(
          s"Invalid format of linearly increasing delay: $s; it should be <integer number>+<integer number>"))

      case ExponentialDelayEx(delayStr, increaseStr) =>
        val d = for (delay <- Try(delayStr.toInt); increaseBy <- Try(increaseStr.toDouble))
          yield ExponentialDelay(delay milliseconds, increaseBy)
        d.toOption.orElse(throw new IllegalArgumentException(
          s"Invalid format of exponentially increasing delay: $s; it should be <integer number>*<real number>"))
    }
  }

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042

  val DefaultKeepAliveMillis = 250
  val DefaultMinReconnectionDelayMillis = 1000
  val DefaultMaxReconnectionDelayMillis = 60000
  val DefaultQueryRetryCount = 10
  val DefaultQueryRetryDelay = RetryDelayConf.ExponentialDelay(4 seconds, 1.5d)
  val DefaultConnectTimeoutMillis = 5000
  val DefaultReadTimeoutMillis = 120000
  val DefaultCassandraConnectionCompression = ProtocolOptions.Compression.NONE

  val DefaultCassandraSSLConf = CassandraSSLConf()

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
  val CassandraQueryRetryDelayProperty = "spark.cassandra.query.retry.delay"
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
    CassandraConnectionRpcPortProperty,
    CassandraConnectionNativePortProperty,
    CassandraConnectionLocalDCProperty,
    CassandraConnectionTimeoutProperty,
    CassandraConnectionKeepAliveProperty,
    CassandraMinReconnectionDelayProperty,
    CassandraMaxReconnectionDelayProperty,
    CassandraConnectionCompressionProperty,
    CassandraQueryRetryCountProperty,
    CassandraQueryRetryDelayProperty,
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
    val queryRetryDelay = RetryDelayConf.fromString(conf.get(CassandraQueryRetryDelayProperty, ""))
      .getOrElse(DefaultQueryRetryDelay)
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
      connectionFactory = connectionFactory,
      cassandraSSLConf = cassandraSSLConf,
      queryRetryDelay = queryRetryDelay
    )
  }
}
