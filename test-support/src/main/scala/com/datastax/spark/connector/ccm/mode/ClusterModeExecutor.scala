package com.datastax.spark.connector.ccm.mode

import com.datastax.spark.connector.ccm.CcmConfig

private[ccm] trait ClusterModeExecutor {

  val config: CcmConfig

  def execute(args: String*): Unit

  def executeUnsanitized(args: String*): Unit

  def create(clusterName: String): Unit

  def remove(): Unit

}