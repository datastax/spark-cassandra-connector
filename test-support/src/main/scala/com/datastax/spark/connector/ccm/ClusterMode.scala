package com.datastax.spark.connector.ccm

import com.datastax.spark.connector.ccm.mode.{ClusterModeExecutor, StandardModeExecutor}

sealed trait ClusterMode {
  def executor(config: CcmConfig): ClusterModeExecutor
}

object ClusterMode {

  /** Default behaviour of CCM Cluster.
    *
    * The cluster is created in a dedicated configuration directory which contains configuration files, data and logs.
    * The directory is removed on JVM shutdown.
    */
  object Standard extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new StandardModeExecutor(config)
  }

  object Debug extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = ???
  }

  object Developer extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = ???
  }

}