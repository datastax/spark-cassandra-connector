package com.datastax.spark.connector.ccm.mode

import java.nio.file.{Files, Path}

import com.datastax.spark.connector.ccm.CcmConfig

private[ccm] class DebugModeExecutor(val config: CcmConfig) extends DefaultExecutor {

  override val dir: Path = Files.createTempDirectory("ccm")

  // do not remove db artifacts, stop db instead
  override def remove(): Unit = {
    execute("stop")
  }

}
