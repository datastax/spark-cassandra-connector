package com.datastax.spark.connector.ccm.mode

import com.datastax.spark.connector.ccm.{CcmBridge, CcmConfig}
import org.apache.commons.exec.CommandLine

import java.nio.file.{Path, Paths}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.control.NonFatal

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

    getLastLogLines(log, linesCount)
  }

  def getLastLogLines(path: String, linesCount: Int): Seq[String] = synchronized {
    val command = s"tail -$linesCount $path"
    CcmBridge.execute(CommandLine.parse(command))
  }

  /**
    * Waits for the node to become alive. The first check is performed after the first interval.
    */
  def waitForNode(nodeNo: Int, timeout: FiniteDuration, interval: Duration = 5.seconds): Boolean = {
    val deadline = timeout.fromNow
    while (!deadline.isOverdue()) {
      Thread.sleep(interval.toMillis)
      if (isAlive(nodeNo, interval)) {
        return true
      }
    }
    false;
  }

  private def isAlive(nodeNo: Int, timeout: Duration): Boolean = {
    import java.net.Socket
    val address = config.addressOfNode(nodeNo)
    val socket = new Socket
    try {
      socket.connect(address, timeout.toMillis.toInt)
      socket.close()
      true
    } catch {
      case NonFatal(_) =>
        false
    }
  }
}