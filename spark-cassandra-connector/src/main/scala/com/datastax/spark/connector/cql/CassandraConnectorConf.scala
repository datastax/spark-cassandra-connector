package com.datastax.spark.connector.cql

import java.net.InetAddress

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkConf}

import com.datastax.driver.core.{ProtocolOptions, SSLOptions}
import com.datastax.spark.connector.util.{ConfigParameter, ConfigCheck}

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
    enabledAlgorithms: Set[String] = SSLOptions.DEFAULT_SSL_CIPHER_SUITES.toSet
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


  val ReferenceSection = "Cassandra Connection Parameters"

  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val DefaultCassandraConnectionHost = "localhost"
  val CassandraConnectionHostDescription = """Contact point to connect to the Cassandra cluster"""
  val ConnectionHostParam = ConfigParameter(
    CassandraConnectionHostProperty,
    ReferenceSection,
    Some(DefaultCassandraConnectionHost),
    CassandraConnectionHostDescription)

  val CassandraConnectionPortProperty = "spark.cassandra.connection.port"
  val DefaultPort = 9042
  val CassandraConnectionPortDescription = """Cassandra native connection port"""
  val ConnectionPortParam = ConfigParameter(
    CassandraConnectionPortProperty,
    ReferenceSection,
    Some(DefaultPort),
    CassandraConnectionPortDescription)

  val CassandraConnectionLocalDCProperty = "spark.cassandra.connection.local_dc"
  val CassandraConnectionLocalDCDescription = """The local DC to connect to (other nodes will be ignored)"""
  val LocalDCParam = ConfigParameter(
    CassandraConnectionLocalDCProperty,
    ReferenceSection,
    None,
    CassandraConnectionLocalDCDescription)

  val CassandraConnectionTimeoutProperty = "spark.cassandra.connection.timeout_ms"
  val DefaultConnectTimeoutMillis = 5000
  val CassandraConnectionTimeoutDescription = """Maximum period of time to attempt connecting to a node"""
  val ConnectionTimeoutParam = ConfigParameter(
    CassandraConnectionTimeoutProperty,
    ReferenceSection,
    Some(DefaultConnectTimeoutMillis),
    CassandraConnectionTimeoutDescription)

  val CassandraConnectionKeepAliveProperty = "spark.cassandra.connection.keep_alive_ms"
  val DefaultKeepAliveMillis = 250
  val CassandraConnectionKeepAliveDescription = """Period of time to keep unused connections open"""
  val KeepAliveMillisParam = ConfigParameter(
    CassandraConnectionKeepAliveProperty,
    ReferenceSection,
    Some(DefaultKeepAliveMillis),
    CassandraConnectionKeepAliveDescription)

  val CassandraMinReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.min"
  val DefaultMinReconnectionDelayMillis = 1000
  val CassandraMinReconnectionDelayDescription = """Minimum period of time to wait before reconnecting to a dead node"""
  val MinReconnectionDelayParam = ConfigParameter(
    CassandraMinReconnectionDelayProperty,
    ReferenceSection,
    Some(DefaultMinReconnectionDelayMillis),
    CassandraMinReconnectionDelayDescription)

  val CassandraMaxReconnectionDelayProperty = "spark.cassandra.connection.reconnection_delay_ms.max"
  val DefaultMaxReconnectionDelayMillis = 60000
  val CassandraMaxReconnectionDelayDescription = """Maximum period of time to wait before reconnecting to a dead node"""
  val MaxReconnectionDelayParam = ConfigParameter(
    CassandraMaxReconnectionDelayProperty,
    ReferenceSection,
    Some(DefaultMaxReconnectionDelayMillis),
    CassandraMaxReconnectionDelayDescription)

  val CassandraConnectionCompressionProperty = "spark.cassandra.connection.compression"
  val DefaultCassandraConnectionCompression = ProtocolOptions.Compression.NONE
  val CassandraConnectionCompressionDescription = """Compression to use (LZ4, SNAPPY or NONE)"""
  val CompressionParam = ConfigParameter(
    CassandraConnectionCompressionProperty,
    ReferenceSection,
    Some("NONE"), // Enum doesn't print correctly
    CassandraConnectionCompressionDescription)

  val CassandraQueryRetryCountProperty = "spark.cassandra.query.retry.count"
  val DefaultQueryRetryCount = 10
  val CassandraQueryRetryCountDescription = """Number of times to retry a timed-out query"""
  val QueryRetryParam = ConfigParameter(
    CassandraQueryRetryCountProperty,
    ReferenceSection,
    Some(DefaultQueryRetryCount),
    CassandraQueryRetryCountDescription)

  val CassandraQueryRetryDelayProperty = "spark.cassandra.query.retry.delay"
  val DefaultQueryRetryDelay = RetryDelayConf.ExponentialDelay(4 seconds, 1.5d)
  val CassandraQueryRetryDelayDescription =
    """The delay between subsequent retries (can be constant,
      | like 1000; linearly increasing, like 1000+100; or exponential, like 1000*2)""".stripMargin
  val QueryRetryDelayParam = ConfigParameter(
    CassandraQueryRetryDelayProperty,
    ReferenceSection,
    Some("4*1.5"), // To string doesn't match input format
    CassandraQueryRetryDelayDescription)

  val CassandraReadTimeoutProperty = "spark.cassandra.read.timeout_ms"
  val DefaultReadTimeoutMillis = 120000
  val CassandraReadTimeoutDescription =""" Maximum period of time to wait for a read to return """
  val ReadTimeoutParam = ConfigParameter(
    CassandraReadTimeoutProperty,
    ReferenceSection,
    Some(DefaultReadTimeoutMillis),
    CassandraReadTimeoutDescription)


  val ReferenceSectionSSL = "Cassandra SSL Connection Options"
  val DefaultCassandraSSLConf = CassandraSSLConf()

  val CassandraConnectionSSLEnabledProperty = "spark.cassandra.connection.ssl.enabled"
  val DefaultSSLEnabled = DefaultCassandraSSLConf.enabled
  val CassandraConnectionSSLEnabledDescription = """Enable secure connection to Cassandra cluster	"""
  val SSLEnabledParam = ConfigParameter(
    CassandraConnectionSSLEnabledProperty,
    ReferenceSectionSSL,
    Some(DefaultSSLEnabled),
    CassandraConnectionSSLEnabledDescription)

  val CassandraConnectionSSLTrustStorePathProperty = "spark.cassandra.connection.ssl.trustStore.path"
  val DefaultSSLTrustStorePath = DefaultCassandraSSLConf.trustStorePath
  val CassandraConnectionSSLTrustStorePathDescription = """Path for the trust store being used"""
  val SSLTrustStorePathParam = ConfigParameter(
    CassandraConnectionSSLTrustStorePathProperty,
    ReferenceSectionSSL,
    DefaultSSLTrustStorePath,
    CassandraConnectionSSLTrustStorePathDescription)

  val CassandraConnectionSSLTrustStorePasswordProperty = "spark.cassandra.connection.ssl.trustStore.password"
  val DefaultSSLTrustStorePassword = DefaultCassandraSSLConf.trustStorePassword
  val CassandraConnectionTrustStorePasswordDescription = """Trust store password"""
  val SSLTrustStorePasswordParam = ConfigParameter(
    CassandraConnectionSSLTrustStorePasswordProperty,
    ReferenceSectionSSL,
    DefaultSSLTrustStorePassword,
    CassandraConnectionTrustStorePasswordDescription)

  val CassandraConnectionSSLTrustStoreTypeProperty = "spark.cassandra.connection.ssl.trustStore.type"
  val DefaultSSLTrustStoreType = DefaultCassandraSSLConf.trustStoreType
  val CassandraConnectionTrustStoreTypeDescription = """Trust store type"""
  val SSLTrustStoreTypeParam = ConfigParameter(
    CassandraConnectionSSLTrustStoreTypeProperty,
    ReferenceSectionSSL,
    Some(DefaultSSLTrustStoreType),
    CassandraConnectionTrustStoreTypeDescription)

  val CassandraConnectionSSLProtocolProperty = "spark.cassandra.connection.ssl.protocol"
  val DefaultSSLProtocol = DefaultCassandraSSLConf.protocol
  val CassandraConnectionSSLProtocolDescription = """SSL protocol"""
  val SSLProtocolParam = ConfigParameter(
    CassandraConnectionSSLProtocolProperty,
    ReferenceSectionSSL,
    Some(DefaultSSLProtocol),
    CassandraConnectionSSLProtocolDescription)

  val CassandraConnectionSSLEnabledAlgorithmsProperty = "spark.cassandra.connection.ssl.enabledAlgorithms"
  val DefaultSSLEnabledAlgorithms = DefaultCassandraSSLConf.enabledAlgorithms
  val CassandraConnectionSSLEnabledAlgorithmsDescription = """SSL cipher suites"""
  val SSLEnabledAlgorithmsParam = ConfigParameter(
    CassandraConnectionSSLEnabledAlgorithmsProperty,
    ReferenceSectionSSL,
    Some(DefaultSSLEnabledAlgorithms.mkString("<br>")),
    CassandraConnectionSSLEnabledDescription)

  //Whitelist for allowed CassandraConnector environment variables
  val Properties:Set[ConfigParameter] = Set(
    ConnectionHostParam,
    ConnectionPortParam,
    LocalDCParam,
    ConnectionTimeoutParam,
    KeepAliveMillisParam,
    MinReconnectionDelayParam,
    MaxReconnectionDelayParam,
    CompressionParam,
    QueryRetryParam,
    QueryRetryDelayParam,
    ReadTimeoutParam,
    SSLEnabledParam,
    SSLTrustStoreTypeParam,
    SSLTrustStorePathParam,
    SSLTrustStorePasswordParam,
    SSLProtocolParam,
    SSLEnabledAlgorithmsParam
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
      .map(_.split(",").map(_.trim).toSet).getOrElse(DefaultCassandraSSLConf.enabledAlgorithms)

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
      cassandraSSLConf = cassandraSSLConf,
      queryRetryDelay = queryRetryDelay
    )
  }
}
