package com.datastax.spark.connector.ccm

import com.datastax.spark.connector.ccm.mode.{ClusterModeExecutor, DebugModeExecutor, StandardModeExecutor}

sealed trait ClusterMode {
  def executor(config: CcmConfig): ClusterModeExecutor
}

object ClusterModes {

  /** Default behaviour of CCM Cluster.
    *
    * The cluster is created in a dedicated configuration directory which contains configuration files, data and logs.
    * The directory is removed on JVM shutdown.
    */
  case object Standard extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new StandardModeExecutor(config)
  }

  /** Debug mode of CCM Cluster.
    *
    * The cluster is created in a dedicated directory that is not removed on shutdown. This mode allows to inspect DB
    * artifacts after test execution.
    */
  case object Debug extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new DebugModeExecutor(config)
  }

  case object Developer extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = ???
  }

  def fromEnvVar: ClusterMode = {
    // we could use Reflection lib or scala-reflect (both are not present on CP at the moment)
    val knownModes = Seq(Standard, Debug, Developer)

    sys.env.get("CCM_CLUSTER_MODE").flatMap { modeName =>
      knownModes.collectFirst {
        case c if c.getClass.getName.toLowerCase.contains(modeName.toLowerCase) => c
      }
    }.getOrElse(Standard)
  }

}