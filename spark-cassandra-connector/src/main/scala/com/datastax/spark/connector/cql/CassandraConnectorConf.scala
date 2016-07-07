package com.datastax.spark.connector.cql

import java.net.InetAddress

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.SparkConf

import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.util.{ConfigParameter, ConfigCheck}
import com.datastax.spark.connector.util.Logging

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  hosts: Set[InetAddress],
  port: Int = CassandraConnectorConf.ConnectionPortParam.default,
  authConf: AuthConf = NoAuthConf,
  localDC: Option[String] = None,
  keepAliveMillis: Int = CassandraConnectorConf.KeepAliveMillisParam.default,
  minReconnectionDelayMillis: Int = CassandraConnectorConf.MinReconnectionDelayParam.default,
  maxReconnectionDelayMillis: Int = CassandraConnectorConf.MaxReconnectionDelayParam.default,
  compression: ProtocolOptions.Compression = CassandraConnectorConf.CompressionParam.default,
  queryRetryCount: Int = CassandraConnectorConf.QueryRetryParam.default,
  connectTimeoutMillis: Int = CassandraConnectorConf.ConnectionTimeoutParam.default,
  readTimeoutMillis: Int = CassandraConnectorConf.ReadTimeoutParam.default,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
  cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf,
  @deprecated("delayed retrying has been disabled; see SPARKC-360", "1.2.6, 1.3.2, 1.4.3, 1.5.1")
  queryRetryDelay: CassandraConnectorConf.RetryDelayConf = CassandraConnectorConf.QueryRetryDelayParam.default
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
    enabledAlgorithms: Set[String] = Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
  )

  @deprecated("delayed retrying has been disabled; see SPARKC-360", "1.2.6, 1.3.2, 1.4.3, 1.5.1")
  trait RetryDelayConf {
    def forRetry(retryNumber: Int): Duration
  }

  @deprecated("delayed retrying has been disabled; see SPARKC-360", "1.2.6, 1.3.2, 1.4.3, 1.5.1")
  object RetryDelayConf extends Serializable {

    case class ConstantDelay(delay: Duration) extends RetryDelayConf {
      require(delay.length >= 0, "Delay must not be negative")

      override def forRetry(nbRetry: Int) = delay
      override def toString() = s"${delay.length}"
    }

    case class LinearDelay(initialDelay: Duration, increaseBy: Duration) extends RetryDelayConf {
      require(initialDelay.length >= 0, "Initial delay must not be negative")
      require(increaseBy.length > 0, "Delay increase must be greater than 0")

      override def forRetry(nbRetry: Int) = initialDelay + (increaseBy * (nbRetry - 1).max(0))
      override def toString() = s"${initialDelay.length} + ${increaseBy}"
    }

    case class ExponentialDelay(initialDelay: Duration, increaseBy: Double) extends RetryDelayConf {
      require(initialDelay.length >= 0, "Initial delay must not be negative")
      require(increaseBy > 0, "Delay increase must be greater than 0")

      override def forRetry(nbRetry: Int) =
        (initialDelay.toMillis * math.pow(increaseBy, (nbRetry - 1).max(0))).toLong milliseconds
      override def toString() = s"${initialDelay.length} * $increaseBy"
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

  val ConnectionHostParam = ConfigParameter[String](
    name = "spark.cassandra.connection.host",
    section = ReferenceSection,
    default = "localhost",
    description =
      """Contact point to connect to the Cassandra cluster. A comma seperated list
        |may also be used. ("127.0.0.1,192.168.0.1")
      """.stripMargin)

  val ConnectionPortParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.port",
    section = ReferenceSection,
    default = 9042,
    description = """Cassandra native connection port""")

  val LocalDCParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.local_dc",
    section = ReferenceSection,
    default = None,
    description = """The local DC to connect to (other nodes will be ignored)""")

  val ConnectionTimeoutParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.timeout_ms",
    section = ReferenceSection,
    default = 5000,
    description = """Maximum period of time to attempt connecting to a node""")

  val KeepAliveMillisParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.keep_alive_ms",
    section = ReferenceSection,
    default = 5000,
    description = """Period of time to keep unused connections open""")

  val MinReconnectionDelayParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.reconnection_delay_ms.min",
    section = ReferenceSection,
    default = 1000,
    description = """Minimum period of time to wait before reconnecting to a dead node""")

  val MaxReconnectionDelayParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.reconnection_delay_ms.max",
    section = ReferenceSection,
    default = 60000,
    description = """Maximum period of time to wait before reconnecting to a dead node""")

  val CompressionParam = ConfigParameter[ProtocolOptions.Compression](
    name = "spark.cassandra.connection.compression",
    section = ReferenceSection,
    default = ProtocolOptions.Compression.NONE,
    description = """Compression to use (LZ4, SNAPPY or NONE)""")

  val QueryRetryParam = ConfigParameter[Int](
    name = "spark.cassandra.query.retry.count",
    section = ReferenceSection,
    default = 10,
    description = """Number of times to retry a timed-out query""")

  @deprecated("delayed retrying has been disabled; see SPARKC-360", "1.2.6, 1.3.2, 1.4.3, 1.5.1")
  val QueryRetryDelayParam = ConfigParameter[RetryDelayConf](
    name = "spark.cassandra.query.retry.delay",
    section = ReferenceSection,
    default = RetryDelayConf.ExponentialDelay(4 seconds, 1.5d),
    description = """The delay between subsequent retries (can be constant,
      | like 1000; linearly increasing, like 1000+100; or exponential, like 1000*2)""".stripMargin)

  val ReadTimeoutParam = ConfigParameter[Int](
    name = "spark.cassandra.read.timeout_ms",
    section = ReferenceSection,
    default = 120000,
    description = """Maximum period of time to wait for a read to return """)


  val ReferenceSectionSSL = "Cassandra SSL Connection Options"
  val DefaultCassandraSSLConf = CassandraSSLConf()

  val SSLEnabledParam = ConfigParameter[Boolean](
    name = "spark.cassandra.connection.ssl.enabled",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.enabled,
    description = """Enable secure connection to Cassandra cluster""")

  val SSLTrustStorePathParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.trustStore.path",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.trustStorePath,
    description = """Path for the trust store being used""")

  val SSLTrustStorePasswordParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.trustStore.password",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.trustStorePassword,
    description = """Trust store password""")

  val SSLTrustStoreTypeParam = ConfigParameter[String](
    name = "spark.cassandra.connection.ssl.trustStore.type",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.trustStoreType,
    description = """Trust store type""")

  val SSLProtocolParam = ConfigParameter[String](
    name = "spark.cassandra.connection.ssl.protocol",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.protocol,
    description = """SSL protocol""")

  val CassandraConnectionSSLEnabledAlgorithmsProperty = "spark.cassandra.connection.ssl.enabledAlgorithms"
  val DefaultSSLEnabledAlgorithms = DefaultCassandraSSLConf.enabledAlgorithms
  val CassandraConnectionSSLEnabledAlgorithmsDescription = """SSL cipher suites"""
  val SSLEnabledAlgorithmsParam = ConfigParameter[Set[String]](
    name = "spark.cassandra.connection.ssl.enabledAlgorithms",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.enabledAlgorithms,
    description = """SSL cipher suites""")

  //Whitelist for allowed CassandraConnector environment variables
  val Properties: Set[ConfigParameter[_]] = Set(
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
    val hostsStr = conf.get(ConnectionHostParam.name, InetAddress.getLocalHost.getHostAddress)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName.trim)
    } yield hostAddress
    
    val port = conf.getInt(ConnectionPortParam.name, ConnectionPortParam.default)
    val authConf = AuthConf.fromSparkConf(conf)
    val keepAlive = conf.getInt(KeepAliveMillisParam.name, KeepAliveMillisParam.default)

    val localDC = conf.getOption(LocalDCParam.name)
    val minReconnectionDelay = conf.getInt(MinReconnectionDelayParam.name, MinReconnectionDelayParam.default)
    val maxReconnectionDelay = conf.getInt(MaxReconnectionDelayParam.name, MaxReconnectionDelayParam.default)
    val queryRetryCount = conf.getInt(QueryRetryParam.name, QueryRetryParam.default)
    val connectTimeout = conf.getInt(ConnectionTimeoutParam.name, ConnectionTimeoutParam.default)
    val readTimeout = conf.getInt(ReadTimeoutParam.name, ReadTimeoutParam.default)

    val compression = conf.getOption(CompressionParam.name)
      .map(ProtocolOptions.Compression.valueOf).getOrElse(CompressionParam.default)

    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)

    val sslEnabled = conf.getBoolean(SSLEnabledParam.name, SSLEnabledParam.default)
    val sslTrustStorePath = conf.getOption(SSLTrustStorePathParam.name).orElse(SSLTrustStorePathParam.default)
    val sslTrustStorePassword = conf.getOption(SSLTrustStorePasswordParam.name).orElse(SSLTrustStorePasswordParam.default)
    val sslTrustStoreType = conf.get(SSLTrustStoreTypeParam.name, SSLTrustStoreTypeParam.default)
    val sslProtocol = conf.get(SSLProtocolParam.name, SSLProtocolParam.default)
    val sslEnabledAlgorithms = conf.getOption(SSLEnabledAlgorithmsParam.name)
      .map(_.split(",").map(_.trim).toSet).getOrElse(SSLEnabledAlgorithmsParam.default)

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
