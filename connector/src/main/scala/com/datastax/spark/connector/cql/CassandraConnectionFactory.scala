package com.datastax.spark.connector.cql

import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.time.Duration

import com.datastax.bdp.spark.DseCassandraConnectionFactory
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption._
import com.datastax.oss.driver.api.core.config.{DriverConfigLoader, ProgrammaticDriverConfigLoaderBuilder}
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.{ConfigParameter, ReflectionUtil}
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import scala.util.Try

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

  def connectorConfigBuilder(conf: CassandraConnectorConf, initBuilder: ProgrammaticDriverConfigLoaderBuilder) = {

    // compression option cannot be set to NONE (default)
    def maybeSetCompressionOption(b: ProgrammaticDriverConfigLoaderBuilder) =
      Option(conf.compression).filter(_ != "NONE").map(c => b.withString(PROTOCOL_COMPRESSION, c.toLowerCase)).getOrElse(b)

    val cassandraCoreThreadCount = Math.max(1, Runtime.getRuntime.availableProcessors() - 1)
    val builder = maybeSetCompressionOption(initBuilder)
      .withInt(CONNECTION_POOL_LOCAL_SIZE, conf.localConnectionsPerExecutor.getOrElse(cassandraCoreThreadCount)) // moved from CassandraConnector
      .withInt(CONNECTION_POOL_REMOTE_SIZE, conf.remoteConnectionsPerExecutor.getOrElse(1)) // moved from CassandraConnector
      .withInt(CONNECTION_INIT_QUERY_TIMEOUT, conf.connectTimeoutMillis)
      .withInt(REQUEST_TIMEOUT, conf.readTimeoutMillis)
      .withStringList(CONTACT_POINTS, conf.hosts.map(h => s"${h.getHostAddress}:${conf.port}").toList.asJava)
      .withString(RETRY_POLICY_CLASS, classOf[MultipleRetryPolicy].getCanonicalName)
      .withString(RECONNECTION_POLICY_CLASS, classOf[ExponentialReconnectionPolicy].getCanonicalName)
      .withDuration(RECONNECTION_BASE_DELAY, Duration.ofMillis(conf.minReconnectionDelayMillis))
      .withDuration(RECONNECTION_MAX_DELAY, Duration.ofMillis(conf.maxReconnectionDelayMillis))
      .withString(LOAD_BALANCING_POLICY_CLASS, classOf[LocalNodeFirstLoadBalancingPolicy].getCanonicalName)

    //Add Auth Conf if set
    conf.authConf.authProperites.foldLeft(builder){ case (b, (driverOption, value)) =>
      b.withString(driverOption, value)}
  }

  /** Creates and configures native Cassandra connection */
  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val builder = DriverConfigLoader.programmaticBuilder()
    val loader = connectorConfigBuilder(conf, builder)

    // TODO:
    //        new QueryOptions()
    //          .setRefreshNodeIntervalMillis(0)
    //          .setRefreshNodeListIntervalMillis(0)
    //          .setRefreshSchemaIntervalMillis(0))
    //      .withoutJMXReporting()
    //      .withThreadingOptions(new DaemonThreadingOptions)
    //      .withNettyOptions(new NettyOptions() {
    //        override def onClusterClose(eventLoopGroup: EventLoopGroup): Unit =
    //          eventLoopGroup.shutdownGracefully(conf.quietPeriodBeforeCloseMillis, conf.timeoutBeforeCloseMillis, TimeUnit.MILLISECONDS)
    //            .syncUninterruptibly()
    //      })
    //    if (conf.cassandraSSLConf.enabled) {
    //      maybeCreateSSLOptions(conf.cassandraSSLConf) match {
    //        case Some(sslOptions) ⇒ builder.withSSL(sslOptions)
    //        case None ⇒ builder.withSSL()
    //      }
    //    } else {
    //      builder
    //    }


    CqlSession.builder()
      .withConfigLoader(loader.build())
      .build()
  }

  private def getKeyStore(
                           ksType: String,
                           ksPassword: Option[String],
                           ksPath: Option[Path]): Option[KeyStore] = {

    ksPath match {
      case Some(path) =>
        val ksIn = Files.newInputStream(path)
        try {
          val keyStore = KeyStore.getInstance(ksType)
          keyStore.load(ksIn, ksPassword.map(_.toCharArray).orNull)
          Some(keyStore)
        } finally {
          Try(ksIn.close())
        }
      case None => None
    }
  }

  /* TODO:
  private def maybeCreateSSLOptions(conf: CassandraSSLConf): Option[SSLOptions] = {
    lazy val trustStore =
      getKeyStore(conf.trustStoreType, conf.trustStorePassword, conf.trustStorePath.map(Paths.get(_)))
    lazy val keyStore =
      getKeyStore(conf.keyStoreType, conf.keyStorePassword, conf.keyStorePath.map(Paths.get(_)))

    if (conf.enabled) {
      val trustManagerFactory = for (ts <- trustStore) yield {
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(ts)
        tmf
      }

      val keyManagerFactory = if (conf.clientAuthEnabled) {
        for (ks <- keyStore) yield {
          val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
          kmf.init(ks, conf.keyStorePassword.map(_.toCharArray).orNull)
          kmf
        }
      } else {
        None
      }

      val context = SSLContext.getInstance(conf.protocol)
      context.init(
        keyManagerFactory.map(_.getKeyManagers).orNull,
        trustManagerFactory.map(_.getTrustManagers).orNull,
        new SecureRandom)

      Some(
        JdkSSLOptions.builder()
          .withSSLContext(context)
          .withCipherSuites(conf.enabledAlgorithms.toArray)
          .build())
    } else {
      None
    }
  }
  */
}

/** Entry point for obtaining `CassandraConnectionFactory` object from [[org.apache.spark.SparkConf SparkConf]],
  * used when establishing connections to Cassandra. */
object CassandraConnectionFactory {
  /* TODO:
  class DaemonThreadingOptions extends ThreadingOptions {
    override def createThreadFactory(clusterName: String, executorName: String): ThreadFactory =
    {
      return new ThreadFactoryBuilder()
        .setNameFormat(clusterName + "-" + executorName + "-%d")
        // Back with Netty's thread factory in order to create FastThreadLocalThread instances. This allows
        // an optimization around ThreadLocals (we could use DefaultThreadFactory directly but it creates
        // slightly different thread names, so keep we keep a ThreadFactoryBuilder wrapper for backward
        // compatibility).
        .setThreadFactory(new DefaultThreadFactory("ignored name"))
        .setDaemon(true)
        .build();
    }
  }
   */

  val ReferenceSection = CassandraConnectorConf.ReferenceSection
  """Name of a Scala module or class implementing
    |CassandraConnectionFactory providing connections to the Cassandra cluster""".stripMargin

  val FactoryParam = ConfigParameter[CassandraConnectionFactory](
    name = "spark.cassandra.connection.factory",
    section = ReferenceSection,
    default = DseCassandraConnectionFactory,
    description =
      """Name of a Scala module or class implementing
        |CassandraConnectionFactory providing connections to the Cassandra cluster""".stripMargin)

  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    conf.getOption(FactoryParam.name)
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(FactoryParam.default)
  }
}