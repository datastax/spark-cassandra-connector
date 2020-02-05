/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import java.time.Duration

import com.datastax.dse.driver.api.core.config.{DseDriverConfigLoader, DseDriverOption}
import com.datastax.dse.driver.api.core.{DseProtocolVersion, DseSession}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter}
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory

object DseCassandraConnectionFactory extends CassandraConnectionFactory {
  @transient
  lazy private val logger = LoggerFactory.getLogger("com.datastax.bdp.spark.DseCassandraConnectionFactory")

  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val loader = DefaultConnectionFactory.connectorConfigBuilder(conf, DseDriverConfigLoader.programmaticBuilder())
      .withDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofMillis(conf.readTimeoutMillis))
      .withDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofMillis(conf.readTimeoutMillis))

    val builder = DseSession.builder()
      .withConfigLoader(loader.build())

    conf.authConf.authProvider.foreach(builder.withAuthProvider)
    builder.withSchemaChangeListener(new MultiplexingSchemaListener())

    builder.build()
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

  def continuousPagingEnabled(session: CqlSession): Boolean = {
    val confEnabled = SparkEnv.get.conf.getBoolean(continuousPagingParam.name, continuousPagingParam.default)
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