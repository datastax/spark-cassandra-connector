/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.spark.connector.cql

import java.util

import com.datastax.dse.driver.api.core.DseSession
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.metadata.Node

/**
  * A Custom Connection Factory for using the Dse Resource Manager
  * Uses the DseCassandraConnectionFactory but uses a SparkNodeOnlyFilter
  */
case object SparkNodeOnlyConnectionFactory extends CassandraConnectionFactory {
  val AnalyticsWorkload: String = "Analytics"

  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val loader = DefaultConnectionFactory.connectorLoader(conf)
      .withString(DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS, classOf[SparkNodeOnlyFilter].getCanonicalName)
      .build()

    DseSession.builder()
      .withConfigLoader(loader)
      .build()
  }
}

/**
  * A Filter which only directs requests at Analytics enabled nodes
  */
class SparkNodeOnlyFilter(driverContext: DriverContext) extends java.util.function.Predicate[Node] {

  private val localDataCenter = driverContext.getConfig.getDefaultProfile.getString(LOAD_BALANCING_LOCAL_DATACENTER).trim

  assert(!localDataCenter.isEmpty, "Local data center must not be empty. Inspect your config, set your local data center.")

  private def isAnalyticsWorkload(node: Node) = {
    val workloads = Option(node.getExtras.get(DseNodeProperties.DSE_WORKLOADS))
    workloads.exists(_.asInstanceOf[util.Set[String]].contains(SparkNodeOnlyConnectionFactory.AnalyticsWorkload))
  }

  override def test(node: Node): Boolean = {
    localDataCenter == node.getDatacenter && isAnalyticsWorkload(node)
  }
}

