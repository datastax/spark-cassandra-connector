package com.datastax.spark.connector.cluster

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.{CcmBridge, CcmConfig}

/** Cluster facade used by test code. */
case class Cluster(
    name: String,
    private[cluster] val config: CcmConfig,
    private[cluster] val ccmBridge: CcmBridge,
    private val nodeConnectionParams: InetSocketAddress => Map[String, String]) {

  val addresses: Seq[InetSocketAddress] = config.nodeAddresses()

  def flush(): Unit = config.nodes.foreach(ccmBridge.flush)

  def getCassandraVersion: Version = ccmBridge.getCassandraVersion
  def getDseVersion: Option[Version] = ccmBridge.getDseVersion

  def refreshSizeEstimates(): Unit = {
    flush()
    config.nodes.foreach(ccmBridge.refreshSizeEstimates)
  }

  def getConnectionHost: String = addresses.head.getHostName

  def getConnectionPort: String = addresses.head.getPort.toString

  def connectionParameters: Map[String, String] = {
    connectionParameters(nodeNo = 0)
  }

  def connectionParameters(nodeNo: Int): Map[String, String] = {
    if (nodeNo >= 0 && nodeNo < addresses.size)
      nodeConnectionParams(addresses(nodeNo))
    else
      throw new IllegalArgumentException(s"Cluster $name has ${addresses.size} nodes, node $nodeNo does not exist.")
  }
}
