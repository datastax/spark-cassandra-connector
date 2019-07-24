package com.datastax.spark.connector.ccm.mode

import com.datastax.spark.connector.ccm.CcmConfig

private[ccm] class DebugModeExecutor(val config: CcmConfig) extends DefaultCreateExecutor with DedicatedDirectoryExecutor {

  // do not remove db artifacts, stop db instead
  override def remove(): Unit = {
    execute("stop")
  }

}
