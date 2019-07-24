package com.datastax.spark.connector.ccm.mode

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.{CcmBridge, CcmConfig}
import org.apache.commons.exec.CommandLine

class StandardModeExecutor(config: CcmConfig) extends ClusterModeExecutor {

  private val configDirectory = Files.createTempDirectory("ccm")
  configDirectory.toFile.deleteOnExit()

  private val created = new AtomicBoolean()

  def execute(args: String*): Unit = synchronized {
    val command = s"ccm ${args.mkString(" ")} --config-dir=${configDirectory.toFile.getAbsolutePath}"
    CcmBridge.execute(CommandLine.parse(command))
  }

  def executeUnsanitized(args: String*): Unit = synchronized {
    val cli = CommandLine.parse("ccm ")
    args.foreach { arg =>
      cli.addArgument(arg, false)
    }
    cli.addArgument("--config-dir=" + configDirectory.toFile.getAbsolutePath)

    CcmBridge.execute(cli)
  }

  def create(clusterName: String): Unit = {
    if (created.compareAndSet(false, true)) {
      val options = config.installDirectory.map(dir => config.createOptions :+ s"--install-dir=${new File(dir).getAbsolutePath}")
        .orElse(config.installBranch.map(branch => config.createOptions :+ s"-v git:${branch.trim().replaceAll("\"", "")}"))
        .getOrElse(config.createOptions :+ config.version.toString)

      execute(
        "create",
        clusterName,
        "-i",
        config.ipPrefix,
        (if (config.dseEnabled) options :+ "--dse" else options).mkString(" "))

      config.nodes.foreach { i =>
        execute("add", "-s", "-j", config.jmxPort(i).toString, "--dse", "-i", config.ipOfNode(i), "--remote-debug-port=0", s"node$i")
      }

      config.cassandraConfiguration.foreach { case (key, value) =>
        execute("updateconf", s"$key:$value")
      }
      if (config.getCassandraVersion.compareTo(Version.V2_2_0) >= 0) {
        execute("updateconf", "enable_user_defined_functions:true")
      }
      if (config.dseEnabled) {
        config.dseConfiguration.foreach { case (key, value) =>
          execute("updatedseconf", s"$key:$value")
        }
        config.dseRawYaml.foreach { yaml =>
          executeUnsanitized("updatedseconf", "-y", yaml)
        }
        if (config.dseWorkloads.nonEmpty) {
          execute("setworkload", config.dseWorkloads.mkString(","))
        }
      }
    }
  }

  def remove(): Unit = {
    execute("remove")
  }

}
