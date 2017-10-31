package com.datastax.spark.connector.cql

import java.net.InetAddress
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.commons.codec.binary.Base64

import scala.language.postfixOps
import scala.util.control.NonFatal
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter, Logging}

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
  maxConnectionsPerExecutor: Option[Int] = CassandraConnectorConf.MaxConnectionsPerExecutorParam.default,
  compression: ProtocolOptions.Compression = CassandraConnectorConf.CompressionParam.default,
  queryRetryCount: Int = CassandraConnectorConf.QueryRetryParam.default,
  connectTimeoutMillis: Int = CassandraConnectorConf.ConnectionTimeoutParam.default,
  readTimeoutMillis: Int = CassandraConnectorConf.ReadTimeoutParam.default,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
  cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf
) {

  @transient
  lazy val serializedConfString: String = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    // Ignore maxConnectionsPerExecutor when comparing Connection Confs
    oos.writeObject(this.copy(maxConnectionsPerExecutor = None));
    oos.close;
    Base64.encodeBase64String(baos.toByteArray)
  }

  override def hashCode: Int = serializedConfString.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: CassandraConnectorConf => that.serializedConfString == serializedConfString
      case _ => false
    }
  }
}

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
    enabledAlgorithms: Set[String] = Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"),
    clientAuthEnabled: Boolean = false,
    keyStorePath: Option[String] = None,
    keyStorePassword: Option[String] = None,
    keyStoreType: String = "JKS"
  )

  val ReferenceSection = "Cassandra Connection Parameters"

  val ConnectionHostParam = ConfigParameter[String](
    name = "spark.cassandra.connection.host",
    section = ReferenceSection,
    default = "localhost",
    description =
      """Contact point to connect to the Cassandra cluster. A comma separated list
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

  val MaxConnectionsPerExecutorParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.connection.connections_per_executor_max",
    section = ReferenceSection,
    default = None,
    description =
      """Maximum number of connections per Host set on each Executor JVM. Will be
        |updated to DefaultParallelism / Executors for Spark Commands. Defaults to 1
        | if not specifying and not in a Spark Env""".stripMargin
  )

  val CompressionParam = ConfigParameter[ProtocolOptions.Compression](
    name = "spark.cassandra.connection.compression",
    section = ReferenceSection,
    default = ProtocolOptions.Compression.NONE,
    description = """Compression to use (LZ4, SNAPPY or NONE)""")

  val QueryRetryParam = ConfigParameter[Int](
    name = "spark.cassandra.query.retry.count",
    section = ReferenceSection,
    default = 60,
    description =
      """Number of times to retry a timed-out query,
        |Setting this to -1 means unlimited retries""".stripMargin)

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

  val SSLClientAuthEnabledParam = ConfigParameter[Boolean](
    name = "spark.cassandra.connection.ssl.clientAuth.enabled",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.clientAuthEnabled,
    description = """Enable 2-way secure connection to Cassandra cluster""")

  val SSLKeyStorePathParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.keyStore.path",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.keyStorePath,
    description = """Path for the key store being used""")

  val SSLKeyStorePasswordParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.connection.ssl.keyStore.password",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.keyStorePassword,
    description = """Key store password""")

  val SSLKeyStoreTypeParam = ConfigParameter[String](
    name = "spark.cassandra.connection.ssl.keyStore.type",
    section = ReferenceSectionSSL,
    default = DefaultCassandraSSLConf.keyStoreType,
    description = """Key store type""")

  //Whitelist for allowed CassandraConnector environment variables
  val Properties: Set[ConfigParameter[_]] = Set(
    ConnectionHostParam,
    ConnectionPortParam,
    LocalDCParam,
    ConnectionTimeoutParam,
    KeepAliveMillisParam,
    MinReconnectionDelayParam,
    MaxReconnectionDelayParam,
    MaxConnectionsPerExecutorParam,
    CompressionParam,
    QueryRetryParam,
    ReadTimeoutParam,
    SSLEnabledParam,
    SSLTrustStoreTypeParam,
    SSLTrustStorePathParam,
    SSLTrustStorePasswordParam,
    SSLProtocolParam,
    SSLEnabledAlgorithmsParam,
    SSLClientAuthEnabledParam,
    SSLKeyStorePathParam,
    SSLKeyStorePasswordParam,
    SSLKeyStoreTypeParam
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
    val hostsStr = conf.get(ConnectionHostParam.name, ConnectionHostParam.default)
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
    val maxConnections = conf.getOption(MaxConnectionsPerExecutorParam.name).map(_.toInt)
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
    val sslClientAuthEnabled = conf.getBoolean(SSLClientAuthEnabledParam.name, SSLClientAuthEnabledParam.default)
    val sslKeyStorePath = conf.getOption(SSLKeyStorePathParam.name).orElse(SSLKeyStorePathParam.default)
    val sslKeyStorePassword = conf.getOption(SSLKeyStorePasswordParam.name).orElse(SSLKeyStorePasswordParam.default)
    val sslKeyStoreType = conf.get(SSLKeyStoreTypeParam.name, SSLKeyStoreTypeParam.default)

    val cassandraSSLConf = CassandraSSLConf(
      enabled = sslEnabled,
      trustStorePath = sslTrustStorePath,
      trustStorePassword = sslTrustStorePassword,
      trustStoreType = sslTrustStoreType,
      clientAuthEnabled = sslClientAuthEnabled,
      keyStorePath = sslKeyStorePath,
      keyStorePassword = sslKeyStorePassword,
      keyStoreType = sslKeyStoreType,
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
      maxConnectionsPerExecutor = maxConnections,
      compression = compression,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory,
      cassandraSSLConf = cassandraSSLConf
    )
  }
}
