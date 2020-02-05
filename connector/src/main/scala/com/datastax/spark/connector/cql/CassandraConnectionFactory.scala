package com.datastax.spark.connector.cql

import java.time.Duration

import com.datastax.bdp.spark.ContinuousPagingScanner
import com.datastax.dse.driver.api.core.DseProtocolVersion
import com.datastax.dse.driver.api.core.config.DseDriverOption
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption._
import com.datastax.oss.driver.api.core.config.{DriverConfigLoader, ProgrammaticDriverConfigLoaderBuilder}
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter, ReflectionUtil}
import org.apache.spark.{SparkConf, SparkEnv}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory` option. */
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures native Cassandra connection */
  def createSession(conf: CassandraConnectorConf): CqlSession

  /** List of allowed custom property names passed in SparkConf */
  def properties: Set[String] = Set.empty

  def getScanner(
                  readConf: ReadConf,
                  connConf: CassandraConnectorConf,
                  columnNames: IndexedSeq[String]): Scanner =
    new DefaultScanner(readConf, connConf, columnNames)

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object DefaultConnectionFactory extends CassandraConnectionFactory {
  @transient
  lazy private val logger = LoggerFactory.getLogger("com.datastax.spark.connector.cql.CassandraConnectionFactory")

  def connectorConfigBuilder(conf: CassandraConnectorConf, initBuilder: ProgrammaticDriverConfigLoaderBuilder) = {
    type LoaderBuilder = ProgrammaticDriverConfigLoaderBuilder

    def basicProperties(builder: LoaderBuilder): LoaderBuilder = {
      val cassandraCoreThreadCount = Math.max(1, Runtime.getRuntime.availableProcessors() - 1)
      builder
        .withInt(CONNECTION_POOL_LOCAL_SIZE, conf.localConnectionsPerExecutor.getOrElse(cassandraCoreThreadCount)) // moved from CassandraConnector
        .withInt(CONNECTION_POOL_REMOTE_SIZE, conf.remoteConnectionsPerExecutor.getOrElse(1)) // moved from CassandraConnector
        .withInt(CONNECTION_INIT_QUERY_TIMEOUT, conf.connectTimeoutMillis)
        .withInt(REQUEST_TIMEOUT, conf.readTimeoutMillis)
        .withStringList(CONTACT_POINTS, conf.hosts.map(h => s"${h.getHostAddress}:${conf.port}").toList.asJava)
        .withClass(RETRY_POLICY_CLASS, classOf[MultipleRetryPolicy])
        .withClass(RECONNECTION_POLICY_CLASS, classOf[ExponentialReconnectionPolicy])
        .withDuration(RECONNECTION_BASE_DELAY, Duration.ofMillis(conf.minReconnectionDelayMillis))
        .withDuration(RECONNECTION_MAX_DELAY, Duration.ofMillis(conf.maxReconnectionDelayMillis))
        .withClass(LOAD_BALANCING_POLICY_CLASS, classOf[LocalNodeFirstLoadBalancingPolicy])
        .withInt(NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, conf.quietPeriodBeforeCloseMillis / 1000)
        .withInt(NETTY_ADMIN_SHUTDOWN_TIMEOUT, conf.timeoutBeforeCloseMillis / 1000)
        .withInt(NETTY_IO_SHUTDOWN_QUIET_PERIOD, conf.quietPeriodBeforeCloseMillis / 1000)
        .withInt(NETTY_IO_SHUTDOWN_TIMEOUT, conf.timeoutBeforeCloseMillis / 1000)
        .withBoolean(NETTY_DAEMON, true)
        .withInt(MultipleRetryPolicy.MaxRetryCount, conf.queryRetryCount)
        .withDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofMillis(conf.readTimeoutMillis))
        .withDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofMillis(conf.readTimeoutMillis))
    }

    // compression option cannot be set to NONE (default)
    def compressionProperties(b: LoaderBuilder): LoaderBuilder =
      Option(conf.compression).filter(_ != "NONE").map(c => b.withString(PROTOCOL_COMPRESSION, c.toLowerCase)).getOrElse(b)

    def localDCProperty(b: LoaderBuilder): LoaderBuilder =
      conf.localDC.map(b.withString(LOAD_BALANCING_LOCAL_DATACENTER, _)).getOrElse(b)

    // add ssl properties if ssl is enabled
    def sslProperties(builder: LoaderBuilder): LoaderBuilder = {
      def clientAuthEnabled(value: Option[String]) =
        if (conf.cassandraSSLConf.clientAuthEnabled) value else None

      if (conf.cassandraSSLConf.enabled) {
        Seq(
          SSL_TRUSTSTORE_PATH -> conf.cassandraSSLConf.trustStorePath,
          SSL_TRUSTSTORE_PASSWORD -> conf.cassandraSSLConf.trustStorePassword,
          SSL_KEYSTORE_PATH -> clientAuthEnabled(conf.cassandraSSLConf.keyStorePath),
          SSL_KEYSTORE_PASSWORD -> clientAuthEnabled(conf.cassandraSSLConf.keyStorePassword))
          .foldLeft(builder) { case (b, (name, value)) =>
            value.map(b.withString(name, _)).getOrElse(b)
          }
          .withClass(SSL_ENGINE_FACTORY_CLASS, classOf[DefaultSslEngineFactory])
          .withStringList(SSL_CIPHER_SUITES, conf.cassandraSSLConf.enabledAlgorithms.toList.asJava)
          .withBoolean(SSL_HOSTNAME_VALIDATION, false) // TODO: this needs to be configurable by users. Set to false for our integration tests
      } else {
        builder
      }
    }

    Seq[LoaderBuilder => LoaderBuilder](basicProperties, compressionProperties, localDCProperty, sslProperties)
      .foldLeft(initBuilder) { case (builder, properties) => properties(builder) }
  }

  /** Creates and configures native Cassandra connection */
  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
    val configLoader = connectorConfigBuilder(conf, configLoaderBuilder).build()

    val builder = CqlSession.builder()
      .withConfigLoader(configLoader)

    conf.authConf.authProvider.foreach(builder.withAuthProvider)
    builder.withSchemaChangeListener(new MultiplexingSchemaListener())

    builder.build()
  }


  def continuousPagingEnabled(session: CqlSession): Boolean = {
    val confEnabled = SparkEnv.get.conf.getBoolean(CassandraConnectionFactory.continuousPagingParam.name, CassandraConnectionFactory.continuousPagingParam.default)
    val pv = session.getContext.getProtocolVersion
    if (pv.getCode > DseProtocolVersion.DSE_V1.getCode && confEnabled) {
      logger.debug(s"Scan Method Being Set to Continuous Paging")
      true
    } else {
      logger.debug(s"Scan Mode Disabled or Connecting to Non-DSE Cassandra Cluster")
      false
    }
  }

  override def getScanner(
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: scala.IndexedSeq[String]): Scanner = {

    val isContinuousPagingEnabled =
      new CassandraConnector(connConf).withSessionDo { continuousPagingEnabled }

    if (isContinuousPagingEnabled) {
      logger.debug("Using ContinousPagingScanner")
      new ContinuousPagingScanner(readConf, connConf, columnNames)
    } else {
      logger.debug("Not Connected to DSE 5.1 or Greater Falling back to Non-Continuous Paging")
      new DefaultScanner(readConf, connConf, columnNames)
    }
  }

}

/** Entry point for obtaining `CassandraConnectionFactory` object from [[org.apache.spark.SparkConf SparkConf]],
  * used when establishing connections to Cassandra. */
object CassandraConnectionFactory {

  val ReferenceSection = CassandraConnectorConf.ReferenceSection
  """Name of a Scala module or class implementing
    |CassandraConnectionFactory providing connections to the Cassandra cluster""".stripMargin

  val FactoryParam = ConfigParameter[CassandraConnectionFactory](
    name = "spark.cassandra.connection.factory",
    section = ReferenceSection,
    default = DefaultConnectionFactory,
    description =
      """Name of a Scala module or class implementing
        |CassandraConnectionFactory providing connections to the Cassandra cluster""".stripMargin)

  val continuousPagingParam = ConfigParameter[Boolean] (
    name = "spark.dse.continuousPagingEnabled",
    section = "Continuous Paging",
    default = true,
    description = "Enables DSE Continuous Paging which improves scanning performance"
  )

  val deprecatedContinuousPagingParam = DeprecatedConfigParameter (
    name = "spark.dse.continuous_paging_enabled",
    replacementParameter = Some(continuousPagingParam),
    deprecatedSince = "DSE 6.0.0"
  )


  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    conf.getOption(FactoryParam.name)
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(FactoryParam.default)
  }

}