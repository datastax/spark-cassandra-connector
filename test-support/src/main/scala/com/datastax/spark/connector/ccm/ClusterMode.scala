package com.datastax.spark.connector.ccm

import com.datastax.spark.connector.ccm.mode.{ClusterModeExecutor, DebugModeExecutor, DeveloperModeExecutor, StandardModeExecutor}

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

  /** CCM Cluster mode that allows to inspect DB artifacts.
    *
    * The cluster is created in a dedicated directory that is not removed on shutdown. This mode allows to inspect DB
    * artifacts after test execution.
    */
  case object Debug extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new DebugModeExecutor(config)
  }

  /** CCM Cluster mode for fixing or developing tests.
    *
    * It can not be used to run tests from different test groups.
    *
    * The cluster is created and started only if it does not exist during test boostrap. The cluster is not stopped on
    * JVM shutdown, it needs manual stop.
    */
  case object Developer extends ClusterMode {
    def executor(config: CcmConfig): ClusterModeExecutor = new DeveloperModeExecutor(config)
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