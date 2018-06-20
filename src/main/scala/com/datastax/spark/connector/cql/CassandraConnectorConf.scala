package com.datastax.spark.connector.cql

import java.net.InetAddress
import java.util.Arrays

import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.spark.SparkConf

import com.datastax.bdp.config.ClientConfiguration
import com.datastax.bdp.transport.common.ServicePrincipal
import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter, DeprecatedConfigParameter, Logging}

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
  localConnectionsPerExecutor: Option[Int] = CassandraConnectorConf.LocalConnectionsPerExecutorParam.default,
  minRemoteConnectionsPerExecutor: Option[Int] = CassandraConnectorConf.MinRemoteConnectionsPerExecutorParam.default,
  maxRemoteConnectionsPerExecutor: Option[Int] = CassandraConnectorConf.MaxRemoteConnectionsPerExecutorParam.default,
  compression: ProtocolOptions.Compression = CassandraConnectorConf.CompressionParam.default,
  queryRetryCount: Int = CassandraConnectorConf.QueryRetryParam.default,
  connectTimeoutMillis: Int = CassandraConnectorConf.ConnectionTimeoutParam.default,
  readTimeoutMillis: Int = CassandraConnectorConf.ReadTimeoutParam.default,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
  cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf
) {

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + port;
    result = prime * result + (if (localDC.isEmpty) 0 else localDC.get.hashCode)
    result = prime * result + keepAliveMillis
    result = prime * result + minReconnectionDelayMillis
    result = prime * result + maxReconnectionDelayMillis
    result = prime * result + queryRetryCount
    result = prime * result + connectTimeoutMillis
    result = prime * result + readTimeoutMillis
    result = prime * result + connectionFactory.getClass.getCanonicalName.hashCode
    result = prime * result + compression.ordinal()
    result = prime * result + cassandraSSLConf.hashCode()
    result = prime * result + authConfHashcode
    return result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: CassandraConnectorConf =>
        that.hosts.map(_.getHostAddress).intersect(this.hosts.map(_.getHostAddress)).nonEmpty &&
        that.port == this.port &&
        equals(that.authConf, this.authConf) &&
        that.localDC == this.localDC &&
        that.keepAliveMillis == this.keepAliveMillis &&
        that.minReconnectionDelayMillis == this.minReconnectionDelayMillis &&
        that.maxReconnectionDelayMillis == this.maxReconnectionDelayMillis &&
        that.queryRetryCount == this.queryRetryCount &&
        that.connectTimeoutMillis == this.connectTimeoutMillis &&
        that.readTimeoutMillis == this.readTimeoutMillis &&
        that.cassandraSSLConf.enabled == this.cassandraSSLConf.enabled &&
        that.cassandraSSLConf.keyStoreType == this.cassandraSSLConf.keyStoreType &&
        that.cassandraSSLConf.keyStorePassword == this.cassandraSSLConf.keyStorePassword &&
        that.cassandraSSLConf.keyStorePath == this.cassandraSSLConf.keyStorePath &&
        that.cassandraSSLConf.trustStorePath == this.cassandraSSLConf.trustStorePath &&
        that.cassandraSSLConf.trustStoreType == this.cassandraSSLConf.trustStoreType &&
        that.cassandraSSLConf.trustStorePassword == this.cassandraSSLConf.trustStorePassword &&
        that.cassandraSSLConf.clientAuthEnabled == this.cassandraSSLConf.clientAuthEnabled &&
        that.cassandraSSLConf.protocol == this.cassandraSSLConf.protocol &&
        that.cassandraSSLConf.enabledAlgorithms == this.cassandraSSLConf.enabledAlgorithms &&
        that.connectionFactory.getClass.getCanonicalName == this.connectionFactory.getClass.getCanonicalName &&
        that.compression.ordinal() == this.compression.ordinal()
      case _ => false
    }
  }

  private def equals(that: AuthConf, thisAuth: AuthConf): Boolean = {
    that match {
      case PasswordAuthConf(user, password) => thisAuth match {
        case PasswordAuthConf(thisUser, thisPassword) => if (user == thisUser && password == thisPassword) true else false
        case _ => false
      }
      case DsePasswordAuthConf(user, password) => thisAuth match {
        case DsePasswordAuthConf(thisUser, thisPassword) => if (user == thisUser && password == thisPassword) true else false
        case _ => false
      }
      case NoAuthConf => thisAuth match {
        case NoAuthConf => true
        case _ => false
      }
      case DseAnalyticsKerberosAuthConf => thisAuth match {
        case DseAnalyticsKerberosAuthConf => true
        case _ => false
      }
      case DseInClusterAuthConf(credentials) => thisAuth match {
        case DseInClusterAuthConf(thisCredentials) =>
          if (Arrays.equals(credentials.password, thisCredentials.password) &&
              credentials.id.code == thisCredentials.id.code &&
              credentials.id.username == thisCredentials.id.username) {
            true
          } else {
            false
          }
        case _ => false
      }
      case ByosAuthConf(clientConfig, tokenStr, credentials) => thisAuth match {
        case ByosAuthConf(thisClientConfig, thisTokenStr, thisCredentials) =>
          if (tokenStr == thisTokenStr && credentials == thisCredentials && equals(clientConfig, thisClientConfig)) true else false
        case _ => false
      }
      case _ => true
    }
  }

  private def equals(that: ClientConfiguration, thisClientConf: ClientConfiguration): Boolean = {
    that == null && thisClientConf == null || that != null && thisClientConf != null &&
    that.isKerberosDefaultScheme == thisClientConf.isKerberosDefaultScheme &&
    that.isKerberosEnabled == thisClientConf.isKerberosEnabled &&
    that.isSslEnabled == thisClientConf.isSslEnabled &&
    that.isSslOptional == thisClientConf.isSslOptional &&
    that.getAdvancedReplicationDirectory == thisClientConf.getAdvancedReplicationDirectory &&
    that.getCassandraHost.getHostAddress == thisClientConf.getCassandraHost.getHostAddress &&
    that.getCdcRawDirectory == thisClientConf.getCdcRawDirectory &&
    that.getCassandraHosts.toSet == thisClientConf.getCassandraHosts.toSet &&
    (that.getCipherSuites != null && thisClientConf.getCipherSuites != null &&
        that.getCipherSuites.toSet == thisClientConf.getCipherSuites.toSet ||
        that.getCipherSuites == null && thisClientConf == null) &&
    that.getDseFsPort == thisClientConf.getDseFsPort &&
    equals(that.getDseServicePrincipal, thisClientConf.getDseServicePrincipal) &&
    equals(that.getHttpServicePrincipal, thisClientConf.getHttpServicePrincipal) &&
    that.getNativePort == thisClientConf.getNativePort &&
    that.getPartitionerClassName == thisClientConf.getPartitionerClassName &&
    that.getSaslProtocolName == thisClientConf.getSaslProtocolName &&
    that.getSslAlgorithm == thisClientConf.getSslAlgorithm &&
    that.getSaslQop == thisClientConf.getSaslQop &&
    that.getSslKeystorePassword == thisClientConf.getSslKeystorePassword &&
    that.getSslKeystorePath == thisClientConf.getSslKeystorePath &&
    that.getSslKeystoreType == thisClientConf.getSslKeystoreType &&
    that.getSslProtocol == thisClientConf.getSslProtocol &&
    that.getSslTruststorePassword == thisClientConf.getSslTruststorePassword &&
    that.getSslTruststorePath == thisClientConf.getSslTruststorePath &&
    that.getSslTruststoreType == thisClientConf.getSslTruststoreType
  }

  private def equals(that: ServicePrincipal, thisPrincipal: ServicePrincipal): Boolean = {
    that == null && thisPrincipal == null ||
    that != null && thisPrincipal != null &&
        that.host == thisPrincipal.host &&
        that.realm == thisPrincipal.realm &&
        that.service == thisPrincipal.service
  }

  private def authConfHashcode(): Int = {
    val prime = 31
    var result = 1
    authConf match {
      case PasswordAuthConf(user, password) =>
        result = prime * result + "PasswordAuthConf".hashCode
        result = prime * result + user.hashCode
        result = prime * result + password.hashCode
      case DsePasswordAuthConf(user, password) =>
        result = prime * result + "DsePasswordAuthConf".hashCode
        result = prime * result + user.hashCode
        result = prime * result + password.hashCode
      case NoAuthConf => result = prime * result + "NoAuthConf".hashCode
      case DseAnalyticsKerberosAuthConf => result = prime * result + "DseAnalyticsKerberosAuthConf".hashCode
      case DseInClusterAuthConf(credentials) =>
        result = prime * result + "DseInClusterAuthConf".hashCode
        result = prime * result + credentials.id.code.hashCode
        result = prime * result + credentials.id.username.hashCode
        result = prime * result + Arrays.hashCode(credentials.password)
      case ByosAuthConf(clientConfig, tokenStr, credentials) =>
        result = prime * result + "ByosAuthConf".hashCode
        result = prime * result + (if (tokenStr.isEmpty) 0 else tokenStr.get.hashCode)
        result = prime * result + (if (credentials.isEmpty) 0 else credentials.get._1.hashCode)
        result = prime * result + (if (credentials.isEmpty) 0 else credentials.get._2.hashCode)
    }
    result
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
    name = "spark.cassandra.connection.localDC",
    section = ReferenceSection,
    default = None,
    description = """The local DC to connect to (other nodes will be ignored)""")

  val DeprecatedLocalDCParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.local_dc",
    replacementParameter = Some(LocalDCParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ConnectionTimeoutParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.timeoutMS",
    section = ReferenceSection,
    default = 5000,
    description = """Maximum period of time to attempt connecting to a node""")

  val DeprecatedConnectionTimeoutParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.timeout_ms",
    replacementParameter = Some(ConnectionTimeoutParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val KeepAliveMillisParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.keepAliveMS",
    section = ReferenceSection,
    default = 5000,
    description = """Period of time to keep unused connections open""")

  val DeprecatedKeepAliveMillisParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.keep_alive_ms",
    replacementParameter = Some(KeepAliveMillisParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val MinReconnectionDelayParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.reconnectionDelayMS.min",
    section = ReferenceSection,
    default = 1000,
    description = """Minimum period of time to wait before reconnecting to a dead node""")

  val DeprecatedMinReconnectionDelayParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.reconnection_delay_ms.min",
    replacementParameter = Some(MinReconnectionDelayParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val MaxReconnectionDelayParam = ConfigParameter[Int](
    name = "spark.cassandra.connection.reconnectionDelayMS.max",
    section = ReferenceSection,
    default = 60000,
    description = """Maximum period of time to wait before reconnecting to a dead node""")

  val LocalConnectionsPerExecutorParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.connection.localConnectionsPerExecutor",
    section = ReferenceSection,
    default = None,
    description =
        """Number of local connections set on each Executor JVM. Defaults to the number
          | of available CPU cores on the local node if not specified and not in a Spark Env""".stripMargin
  )

  val DeprecatedMaxReconnectionDelayParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.reconnection_delay_ms.max",
    replacementParameter = Some(MaxReconnectionDelayParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val MinRemoteConnectionsPerExecutorParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.connection.remoteConnectionsPerExecutorMin",
    section = ReferenceSection,
    default = None,
    description =
        """Minimum number of remote connections per Host set on each Executor JVM. Default value is
          | estimated automatically based on the total number of executors in the cluster""".stripMargin
  )

  val MaxRemoteConnectionsPerExecutorParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.connection.remoteConnectionsPerExecutorMax",
    section = ReferenceSection,
    default = None,
    description =
      """Maximum number of remote connections per Host set on each Executor JVM. Default value is
        | estimated automatically based on the total number of executors in the cluster""".stripMargin
  )

  val MaxConnectionsPerExecutorParam = DeprecatedConfigParameter(
    name = "spark.cassandra.connection.connections_per_executor_max",
    replacementParameter = Some(MaxRemoteConnectionsPerExecutorParam),
    deprecatedSince = "DSE 6.0.0"
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
      """Number of times to retry a timed-out query
        |Setting this to -1 means unlimited retries
      """.stripMargin)

  val ReadTimeoutParam = ConfigParameter[Int](
    name = "spark.cassandra.read.timeoutMS",
    section = ReferenceSection,
    default = 120000,
    description = """Maximum period of time to wait for a read to return """)

  val DeprecatedReadTimeoutParam = DeprecatedConfigParameter(
    name = "spark.cassandra.read.timeout_ms",
    replacementParameter = Some(ReadTimeoutParam),
    deprecatedSince = "DSE 6.0.0"
  )

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
    fromSparkConf(conf)
  }

  def fromSparkConf(conf: SparkConf) = {
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
    val localConnections = conf.getOption(LocalConnectionsPerExecutorParam.name).map(_.toInt)
    val minRemoteConnections = conf.getOption(MinRemoteConnectionsPerExecutorParam.name).map(_.toInt)
    val maxRemoteConnections = conf.getOption(MaxRemoteConnectionsPerExecutorParam.name).map(_.toInt)
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
      localConnectionsPerExecutor = localConnections,
      minRemoteConnectionsPerExecutor = minRemoteConnections,
      maxRemoteConnectionsPerExecutor = maxRemoteConnections,
      compression = compression,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory,
      cassandraSSLConf = cassandraSSLConf
    )
  }
}
