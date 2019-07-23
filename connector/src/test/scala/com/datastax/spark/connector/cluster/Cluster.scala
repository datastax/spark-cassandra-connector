package com.datastax.spark.connector.cluster

import java.net.InetSocketAddress
import java.util.Optional

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmBridge

import scala.collection.JavaConverters._

/** Cluster facade used by test code. */
case class Cluster(
    name: String,
    private[cluster] val ccmBridge: CcmBridge,
    private val nodeConnectionParams: InetSocketAddress => Map[String, String]) {

  def addresses: Seq[InetSocketAddress] = ccmBridge.nodeAddresses().asScala

  def flush(): Unit = ccmBridge.getNodes.foreach(ccmBridge.flush)

  def getDseVersion: Optional[Version] = ccmBridge.getDseVersion

  def refreshSizeEstimates(): Unit = {
    flush()
    ccmBridge.getNodes.foreach(ccmBridge.refreshSizeEstimates)
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
