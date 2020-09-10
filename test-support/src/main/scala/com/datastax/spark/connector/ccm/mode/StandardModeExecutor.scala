package com.datastax.spark.connector.ccm.mode

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

private[mode] trait DefaultExecutor extends ClusterModeExecutor {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StandardModeExecutor])

  private val created = new AtomicBoolean()

  override def start(nodeNo: Int): Unit = {
    val formattedJvmArgs = config.jvmArgs.map(arg => s" --jvm_arg=$arg").mkString(" ")
    execute(s"node$nodeNo", "start", formattedJvmArgs + "--wait-for-binary-proto")
  }

  private def eventually[T](hint: String = "", f: =>T ): T = {
    val start = System.currentTimeMillis()
    val timeoutInSeconds = 20
    val intervalInMs = 500
    val end = start + timeoutInSeconds * 1000

    while ( System.currentTimeMillis() < end) {
      try {
        return f
      } catch { case e: Throwable =>
        logger.warn(s"Tried to execute code, will retry : ${e.getMessage}")
        Thread.sleep(intervalInMs)
      }
    }
    throw new IllegalStateException(s"Unable to complete function in $timeoutInSeconds: $hint")
  }

  /**
    * Remove this once C* 4.0.0 is released.
    *
    * This is a workaround that allows running it:test against 4.0.0-beta1. This version of C* is published as
    * 4.0-beta1 which breaks versioning convention used in integration tests.
    */
  private def adjustCassandraBetaVersion(version: String): String = {
    val beta = "4.0.0-beta(\\d+)".r
    version match {
      case beta(betaNo) => s"4.0-beta$betaNo"
      case other => other
    }
  }

  override def create(clusterName: String): Unit = {
    if (created.compareAndSet(false, true)) {
      val options = config.installDirectory
        .map(dir => config.createOptions :+ s"--install-dir=${new File(dir).getAbsolutePath}")
        .orElse(config.installBranch.map(branch => config.createOptions :+ s"-v git:${branch.trim().replaceAll("\"", "")}"))
        .getOrElse(config.createOptions :+ s"-v ${adjustCassandraBetaVersion(config.version.toString)}")

      val dseFlag = if (config.dseEnabled) Some("--dse") else None

      val createArgs = Seq("create", clusterName, "-i", config.ipPrefix, (options ++ dseFlag).mkString(" "))

      // Check installed Directory
      val repositoryDir = Paths.get(
        sys.props.get("user.home").get,
        ".ccm",
        "repository",
        adjustCassandraBetaVersion(config.getDseVersion.getOrElse(config.getCassandraVersion).toString))

      if (Files.exists(repositoryDir)) {
        logger.info(s"Found cached repository dir: $repositoryDir")
        logger.info("Checking for appropriate bin dir")
        eventually(f = Files.exists(repositoryDir.resolve("bin")))
      }

      execute( createArgs: _*)

      eventually("Checking to make sure repository was correctly expanded", {
        Files.exists(repositoryDir.resolve("bin"))
      })

      config.nodes.foreach { i =>
        val addArgs = Seq ("add",
          "-s",
          "-j", config.jmxPort(i).toString,
          "-i", config.ipOfNode(i),
          "--remote-debug-port=0") ++
          dseFlag :+
          s"node$i"

        execute(addArgs: _*)
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
      } else {
        // C* 4.0.0 has materialized views disabled by default
        if (config.getCassandraVersion.compareTo(Version.parse("4.0-beta1")) >= 0) {
          execute("updateconf", "enable_materialized_views:true")
        }
      }
    }
  }
}

private[ccm] class StandardModeExecutor(val config: CcmConfig) extends DefaultExecutor {
  override val dir: Path = Files.createTempDirectory("ccm")
  // remove config directory on shutdown
  dir.toFile.deleteOnExit()
  // remove db artifacts
  override def remove(): Unit = {
    execute("remove")
  }
}
