package com.datastax.spark.connector.ccm.mode
import com.datastax.spark.connector.ccm.CcmConfig

import java.nio.file.{Files, Path}

/**
 * A special ClusterModeExecutor which bypasses ccm and assumes a Cassandra instance on localhost
 * with default ports and no authentication.
 * */
private[ccm] class ExistingModeExecutor(val config: CcmConfig) extends ClusterModeExecutor {
  override protected val dir: Path = Files.createTempDirectory("test")

  override def create(clusterName: String): Unit = {
    // do nothing
  }

  override def start(nodeNo: Int): Unit = {
    // do nothing
  }

  override def remove(): Unit = {
    // do nothing
  }
}
