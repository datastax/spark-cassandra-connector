/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.spark.connector.cql

import java.net.InetAddress
import java.util

import scala.collection.JavaConverters._

import com.typesafe.scalalogging.StrictLogging

import com.datastax.bdp.spark.DseCassandraConnectionFactory
import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.{Cluster, Host, HostDistance, Statement}

/**
  * A Custom Connection Factory for using the Dse Resource Manager
  * Uses the DseCasasndraConnectionFactory but uses a SparkNodeOnlyLoadBalancingPolicy
  */
case object SparkNodeOnlyConnectionFactory extends CassandraConnectionFactory {
  val AnalyticsWorkload: String = "Analytics"

  override def createCluster(conf: CassandraConnectorConf): Cluster =
    DseCassandraConnectionFactory
      .getClusterBuilder(conf)
      .withLoadBalancingPolicy(new SparkNodeOnlyLoadBalancingPolicy(conf.hosts, conf.localDC))
      .build()
}

/**
  * A Policy which only directs requests at Analytics enabled nodes
  */
class SparkNodeOnlyLoadBalancingPolicy(
    contactPoints: Set[InetAddress],
    localDC: Option[String] = None) extends LoadBalancingPolicy with StrictLogging {

  import SparkNodeOnlyConnectionFactory.AnalyticsWorkload

  //All the nodes
  private var sparkNodes = Set.empty[Host]
  private var dcToUse = ""

  override def newQueryPlan(loggedKeyspace: String, statement: Statement): util.Iterator[Host] = {
    if (sparkNodes.isEmpty) {
      logger.error(
        s"""Unable to route DSE Resource Manager request.
           |None of the currently known nodes in the DC $dcToUse are running an $AnalyticsWorkload workload.
           |Please set the connection.local_dc parameter in your dse:// master URI or
           |choose a contact point in a DC with DSE $AnalyticsWorkload nodes running.""".stripMargin)
      Iterator[Host]().asJava
    } else {
      LocalNodeFirstLoadBalancingPolicy.sortNodesByStatusAndProximity(dcToUse, sparkNodes).iterator.asJava
    }
  }

  override def init(cluster: Cluster, hosts: util.Collection[Host]): Unit = {
    dcToUse = localDC.getOrElse(LocalNodeFirstLoadBalancingPolicy.determineDataCenter(contactPoints, hosts.asScala.toSet))
    sparkNodes = hosts.asScala.filter(sameDCAndAnalytics).toSet
  }

  private def sameDCAndAnalytics(host: Host): Boolean = {
    host.getDseWorkloads.contains(AnalyticsWorkload) && localDC.forall(host.getDatacenter == _)
  }

  override def distance(host: Host): HostDistance = {
    if (LocalNodeFirstLoadBalancingPolicy.isLocalHost(host)) {
      HostDistance.LOCAL
    } else {
      HostDistance.REMOTE
    }
  }

  override def onAdd(host: Host): Unit = {
    if (sameDCAndAnalytics(host)) sparkNodes += host
  }

  override def onRemove(host: Host): Unit = {
    sparkNodes -= host
  }

  override def onUp(host: Host): Unit = {}

  override def onDown(host: Host): Unit = {}

  override def close(): Unit = {}
}

