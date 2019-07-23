package com.datastax.spark.connector.ccm

import java.io.{File, IOException}
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig._
import org.apache.commons.exec.{CommandLine, ExecuteWatchdog, LogOutputStream, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

class CcmBridge(config: CcmConfig) extends AutoCloseable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CcmBridge])

  private val configDirectory = Files.createTempDirectory("ccm")
  configDirectory.toFile.deleteOnExit()

  private val started = new AtomicBoolean()
  private val created = new AtomicBoolean()

  def getDseVersion: Option[Version] = {
    if (config.dseEnablement) Option(config.version) else None
  }

  def getCassandraVersion: Version = {
    if (!config.dseEnablement) {
      config.version
    } else {
      val stableVersion = config.version.nextStable()
      if (stableVersion.compareTo(V6_0_0) >= 0) {
        V4_0_0
      } else if (stableVersion.compareTo(V5_1_0) >= 0) {
        V3_10
      } else if (stableVersion.compareTo(V5_0_0) >= 0) {
        V3_0_15
      } else {
        V2_1_19
      }
    }
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
        (if (config.dseEnablement) options :+ "--dse" else options).mkString(" "))

      nodes().foreach { i =>
        execute("add",
          "-s",
          "-j", jmxPort(i).toString,
          "--dse",
          "-i", ipOfNode(i),
          "--remote-debug-port=0",
          "node" + i)
      }

      config.cassandraConfiguration.foreach { case (key, value) =>
        execute("updateconf", s"$key:$value")
      }
      if (getCassandraVersion.compareTo(Version.V2_2_0) >= 0) {
        execute("updateconf", "enable_user_defined_functions:true")
      }
      if (config.dseEnablement) {
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

  def dsetool(n: Int, args: String*): Unit = {
    execute(s"node$n dsetool ${args.mkString(" ")}")
  }

  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      nodes().foreach(start)
    }
  }

  def stop(): Unit = {
    if (started.compareAndSet(true, false)) {
      execute("stop")
    }
  }

  def remove(): Unit = {
    execute("remove")
  }

  def pause(n: Int): Unit = {
    execute(s"node$n", "pause")
  }

  def resume(n: Int): Unit = {
    execute(s"node$n", "resume")
  }

  def start(n: Int): Unit = {
    val formattedJvmArgs = config.jvmArgs.map(arg => s" --jvm_arg=$arg").mkString(" ")
    execute(s"node$n", "start", formattedJvmArgs + "--wait-for-binary-proto")
  }

  def nodetool(n: Int, args: String*): Unit = {
    execute(s"node$n nodetool ${args.mkString(" < ")}")
  }

  def refreshSizeEstimates(n: Int): Unit = {
    nodetool(n, "refreshsizeestimates")
  }

  def flush(n: Int): Unit = {
    execute("node" + n, "flush")
  }

  def ipOfNode(n: Int): String = {
    config.ipPrefix + n
  }

  def jmxPort(n: Int): Integer = {
    7108 + config.jmxPortOffset + n
  }

  def nodes(): Seq[Int] = {
    config.nodes
  }

  def addressOfNode(n: Int): InetSocketAddress = {
    new InetSocketAddress(ipOfNode(n), 9042)
  }

  def nodeAddresses(): Seq[InetSocketAddress] = {
    config.nodes.map(addressOfNode)
  }

  def stop(n: Int): Unit = {
    execute(s"node$n", "stop")
  }

  def execute(args: String*): Unit = synchronized {
    val command = s"ccm ${args.mkString(" ")} --config-dir=${configDirectory.toFile.getAbsolutePath}"
    execute(CommandLine.parse(command))
  }

  def executeUnsanitized(args: String*): Unit = synchronized {
    val cli = CommandLine.parse("ccm ")
    args.foreach { arg =>
      cli.addArgument(arg, false)
    }
    cli.addArgument("--config-dir=" + configDirectory.toFile.getAbsolutePath)

    execute(cli)
  }

  override def close(): Unit = {
    remove()
  }

  private def execute(cli: CommandLine): Unit = {
    logger.debug("Executing: " + cli)

    val watchDog: ExecuteWatchdog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10))
    val outStream = new LogOutputStream() {
      override def processLine(line: String, logLevel: Int): Unit = logger.debug("ccmout> {}", line)
    }
    val errStream = new LogOutputStream() {
      override def processLine(line: String, logLevel: Int): Unit = logger.error("ccmerr> {}", line)
    }

    try {
      val executor = new DefaultExecutor()
      val streamHandler = new PumpStreamHandler(outStream, errStream)
      executor.setStreamHandler(streamHandler)
      executor.setWatchdog(watchDog)
      val retValue = executor.execute(cli)
      if (retValue != 0) {
        logger.error(
          "Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli)
      }
    } catch {
      case _: IOException if watchDog.killedProcess() =>
        throw new RuntimeException("The command '" + cli + "' was killed after 10 minutes")
      case ex: IOException =>
        throw new RuntimeException("The command '" + cli + "' failed to execute", ex)
    } finally {
      Try(outStream.close())
      Try(errStream.close())
    }
  }

}
