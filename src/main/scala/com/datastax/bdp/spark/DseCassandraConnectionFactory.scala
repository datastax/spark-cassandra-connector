/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import java.util.concurrent.ThreadFactory
import javax.net.ssl.SSLContext

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory

import com.datastax.bdp.config.{ClientConfiguration, DetachedClientConfigurationFactory}
import com.datastax.bdp.util.{DseConnectionUtil, SSLUtil}
import com.datastax.driver.core._
import com.datastax.driver.dse.DseCluster
import com.datastax.driver.dse.graph.{GraphOptions, GraphProtocol}
import com.datastax.driver.extras.codecs.jdk8.{InstantCodec, LocalDateCodec, LocalTimeCodec}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter}

object DseCassandraConnectionFactory extends CassandraConnectionFactory {
  @transient
  lazy private val logger = LoggerFactory.getLogger("com.datastax.bdp.spark.DseCassandraConnectionFactory")

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

  def customCodecRegistry: CodecRegistry = {
    new CodecRegistry()
    .register(LocalDateCodec.instance)
    .register(LocalTimeCodec.instance)
    .register(InstantCodec.instance)
  }

  def dseClusterBuilder(conf: CassandraConnectorConf) = {
    val defBuilder = DefaultConnectionFactory.clusterBuilder(conf)
    val defConf = defBuilder.getConfiguration

    val dseBuilder = DseCluster.builder()
      .addContactPointsWithPorts(defBuilder.getContactPoints)
      .withPort(conf.port)
      .withRetryPolicy(defConf.getPolicies.getRetryPolicy)
      .withReconnectionPolicy(defConf.getPolicies.getReconnectionPolicy)
      .withLoadBalancingPolicy(defConf.getPolicies.getLoadBalancingPolicy)
      .withAuthProvider(defConf.getProtocolOptions.getAuthProvider)
      .withSocketOptions(defConf.getSocketOptions)
      .withCompression(defConf.getProtocolOptions.getCompression)
      .withQueryOptions(defConf.getQueryOptions)
      .withGraphOptions(new GraphOptions().setGraphSubProtocol(GraphProtocol.GRAPHSON_2_0))
      .withThreadingOptions(new DaemonThreadingOptions)
      .withoutJMXReporting()
      .withoutMetrics()
      .withNettyOptions(defConf.getNettyOptions)

    val maybeSSLOptions =  Option(defConf.getProtocolOptions.getSSLOptions)
    maybeSSLOptions match {
      case Some(sslOptions) => dseBuilder.withSSL(sslOptions)
      case None => dseBuilder
    }
  }


  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    getClusterBuilder(conf).build
  }

  def getClusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val builder = dseClusterBuilder(conf)
    Option(conf.authConf.authProvider).foreach(builder.withAuthProvider)
    sslOptions(conf).foreach(builder.withSSL)
    builder
  }

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

  def continuousPagingEnabled(cluster: Cluster): Boolean = {
    val confEnabled = SparkEnv.get.conf.getBoolean(continuousPagingParam.name, continuousPagingParam.default)
    val pv = cluster.getConfiguration.getProtocolOptions.getProtocolVersion
    if (pv.compareTo(ProtocolVersion.DSE_V1) >= 0 && confEnabled) {
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
      new CassandraConnector(connConf)
        .withClusterDo { continuousPagingEnabled }

    if (isContinuousPagingEnabled) {
      logger.debug("Using ContinousPagingScanner")
      new ContinuousPagingScanner(readConf, connConf, columnNames)
    } else {
      logger.debug("Not Connected to DSE 5.1 or Greater Falling back to Non-Continuous Paging")
      new DefaultScanner(readConf, connConf, columnNames)
    }
  }

  def sslOptions(conf: CassandraConnectorConf): Option[SSLOptions] = {
    def buildSSLOptions(clientConf: ClientConfiguration): Option[SSLOptions] = {
      getSSLContext(clientConf).map {
        case sslContext =>
          logger.info("SSL enabled")
          JdkSSLOptions.builder()
              .withSSLContext(sslContext)
              .withCipherSuites(getCipherSuites(clientConf))
              .build()
      }
    }

    val clientConf: Option[ClientConfiguration] = conf.authConf match {
      case byosSslConfig: DseByosAuthConfFactory.ByosAuthConf  => Some(byosSslConfig.clientConfig)
      case _ => try {
        Some(DetachedClientConfigurationFactory.getClientConfiguration())
      } catch {
        case _: Throwable => None
      }
    }

    clientConf.flatMap(buildSSLOptions)
  }

  private def getSSLContext(clientConf: ClientConfiguration): Option[SSLContext] = {
    if (clientConf.isSslEnabled) {
      val tmf = SSLUtil.initTrustManagerFactory(
        clientConf.getSslTruststorePath,
        clientConf.getSslTruststoreType,
        clientConf.getSslTruststorePassword)
      val kmf = Option(clientConf.getSslKeystorePath)
          .map(path => SSLUtil.initKeyManagerFactory(
            path,
            clientConf.getSslKeystoreType,
            clientConf.getSslKeystorePassword,
            clientConf.getSslKeystorePassword))
      val sslContext = SSLUtil.initSSLContext(tmf, kmf.orNull, clientConf.getSslProtocol)
      Some(sslContext)
    } else {
      None
    }
  }

  private def getCipherSuites(clientConf: ClientConfiguration): Array[String] = {
    if (clientConf.getCipherSuites != null && clientConf.getCipherSuites.nonEmpty)
      clientConf.getCipherSuites
    else
      CassandraConnectorConf.SSLEnabledAlgorithmsParam.default.toArray
  }
}
