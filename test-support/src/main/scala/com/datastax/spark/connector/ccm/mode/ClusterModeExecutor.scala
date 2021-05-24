package com.datastax.spark.connector.ccm.mode

import java.nio.file.{Path, Paths}
import com.datastax.spark.connector.ccm.{CcmBridge, CcmConfig}
import org.apache.commons.exec.CommandLine

private[ccm] trait ClusterModeExecutor {

  protected val config: CcmConfig

  protected val dir: Path

  def create(clusterName: String): Unit

  def start(nodeNo: Int): Unit

  def remove(): Unit

  def execute(args: String*): Seq[String] = synchronized {
    val command = s"ccm ${args.mkString(" ")} --config-dir=${dir.toFile.getAbsolutePath}"
    CcmBridge.execute(CommandLine.parse(command))
  }

  def executeUnsanitized(args: String*): Seq[String] = synchronized {
    val cli = CommandLine.parse("ccm ")
    args.foreach { arg =>
      cli.addArgument(arg, false)
    }
    cli.addArgument("--config-dir=" + dir.toFile.getAbsolutePath)

    CcmBridge.execute(cli)
  }

  def getLastRepositoryLogLines(linesCount: Int): Seq[String] = synchronized {
    val log = Paths.get(
      sys.props.get("user.home").get,
      ".ccm",
      "repository",
      "ccm-repository.log").toString

    val command = s"tail -$linesCount $log"
    CcmBridge.execute(CommandLine.parse(command))
  }
}