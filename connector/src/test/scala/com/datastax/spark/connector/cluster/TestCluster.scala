package com.datastax.spark.connector.cluster

import java.net.InetSocketAddress
import java.util.Optional

import scala.collection.JavaConverters._
import com.datastax.spark.connector.ccm.{CcmBridge, Version}

/** Cluster facade used by test code. */
// TODO: rename to cluster once driver is updated to 2.1.0 (where there is no Cluster class in API)
case class TestCluster(
    name: String,
    private[cluster] val ccmBridge: CcmBridge) {

  def addresses: Seq[InetSocketAddress] = ccmBridge.nodeAddresses().asScala

  def flush(): Unit = ccmBridge.getNodes.foreach(ccmBridge.flush)

  def getDseVersion: Optional[Version] = ccmBridge.getDseVersion

  def refreshSizeEstimates(): Unit = {
    flush()
    ccmBridge.getNodes.foreach(ccmBridge.refreshSizeEstimates)
  }

  def getConnectionHost: String = addresses.head.getHostName

  def getConnectionPort: String = addresses.head.getPort.toString
}
