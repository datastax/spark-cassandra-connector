package com.datastax.spark.connector.cql

import java.nio.file.{Files, Path, Paths}
import java.security.{KeyStore, SecureRandom}
import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadFactory
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import io.netty.channel.EventLoopGroup
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf

import com.datastax.driver.core._
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.spark.connector.cql.CassandraConnectionFactory.DaemonThreadingOptions
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.{ConfigParameter, ReflectionUtil}

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory` option. */
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster

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

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    val builder = Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      .withRetryPolicy(
        new MultipleRetryPolicy(conf.queryRetryCount))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
      .withLoadBalancingPolicy(
        new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
      .withCompression(conf.compression)
      .withQueryOptions(
        new QueryOptions()
          .setRefreshNodeIntervalMillis(0)
          .setRefreshNodeListIntervalMillis(0)
          .setRefreshSchemaIntervalMillis(0))
      .withoutJMXReporting()
      .withoutMetrics()
      .withThreadingOptions(new DaemonThreadingOptions)
      .withNettyOptions(new NettyOptions() {
        override def onClusterClose(eventLoopGroup: EventLoopGroup): Unit =
          eventLoopGroup.shutdownGracefully(conf.quietPeriodBeforeCloseMillis, conf.timeoutBeforeCloseMillis, TimeUnit.MILLISECONDS)
            .syncUninterruptibly()
      })

    if (conf.cassandraSSLConf.enabled) {
      maybeCreateSSLOptions(conf.cassandraSSLConf) match {
        case Some(sslOptions) ⇒ builder.withSSL(sslOptions)
        case None ⇒ builder.withSSL()
      }
    } else {
      builder
    }
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
          IOUtils.closeQuietly(ksIn)
        }
      case None => None
    }
  }

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

  /** Creates and configures native Cassandra connection */
  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    clusterBuilder(conf).build()
  }

}

/** Entry point for obtaining `CassandraConnectionFactory` object from [[org.apache.spark.SparkConf SparkConf]],
  * used when establishing connections to Cassandra. */
object CassandraConnectionFactory {
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

  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    conf.getOption(FactoryParam.name)
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(FactoryParam.default)
  }
}
