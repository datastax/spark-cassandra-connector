package com.datastax.spark.connector.ccm.mode

trait ClusterModeExecutor {

  def execute(args: String*): Unit

  def executeUnsanitized(args: String*): Unit

  def create(clusterName: String): Unit

  def remove(): Unit

}