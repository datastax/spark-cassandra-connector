package com.datastax.spark.connector.embedded

import java.net.InetAddress

import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.commons.io.FileUtils

import com.datastax.spark.connector.embedded.YamlTransformations.CassandraConfiguration

private[connector] class CassandraRunner(
    val configTemplate: YamlTransformations,
    val baseConfiguration: CassandraConfiguration)
  extends Embedded {

  import java.io.{File, IOException}

  import com.google.common.io.Files

  import CassandraRunner._

  val tempDir = mkdir(new File(Files.createTempDir(), "spark-cassandra-connector"))
  val workDir = mkdir(new File(tempDir, "cassandra"))
  val dataDir = mkdir(new File(workDir, "data"))
  val commitLogDir = mkdir(new File(workDir, "commitlog"))
  val cachesDir = mkdir(new File(workDir, "saved_caches"))
  val confDir = mkdir(new File(tempDir, "conf"))
  val confFile = new File(confDir, "cassandra.yaml")

  YamlTransformations.makeYaml(confFile.toPath,
    baseConfiguration.copy(cassandraDir = workDir.getAbsolutePath), configTemplate)

  private val classPath = sys.env.get("IT_CASSANDRA_PATH").map { customCassandraDir =>
    val entries = (for (f <- Files.fileTreeTraverser().breadthFirstTraversal(new File(customCassandraDir, "lib")).toIterator
                        if f.isDirectory || f.getName.endsWith(".jar")) yield {
      f.getAbsolutePath
    }).toList ::: new File(customCassandraDir, "conf") :: Nil
    entries.mkString(File.pathSeparator)
  } orElse sys.env.get("CASSANDRA_CLASSPATH") getOrElse System.getProperty("java.class.path")


  private val javaBin = System.getProperty("java.home") + "/bin/java"
  private val cassandraConfProperty = "-Dcassandra.config=file:" + confFile.toString
  private val superuserSetupDelayProperty = "-Dcassandra.superuser_setup_delay_ms=0"
  private val jmxPortProperty = s"-Dcassandra.jmx.local.port=${baseConfiguration.jmxPort}"
  private val sizeEstimatesUpdateIntervalProperty =
    s"-Dcassandra.size_recorder_interval=$SizeEstimatesUpdateIntervalInSeconds"
  private val jammAgent = classPath.split(File.pathSeparator).find(_.matches(".*jamm.*\\.jar"))
  private val jammAgentProperty = jammAgent.map("-javaagent:" + _).getOrElse("")
  private val cassandraMainClass = "org.apache.cassandra.service.CassandraDaemon"
  private val nodeToolMainClass = "org.apache.cassandra.tools.NodeTool"
  private val logConfigFileProperty = s"-Dlog4j.configuration=${getClass.getResource("/log4j.properties").toString}"

  val location = Thread.currentThread().getStackTrace
    .filter(_.getClassName.startsWith("com.datastax")).lastOption
    .map(ste => s"   at ${ste.getFileName}:${ste.getLineNumber} (${ste.getClassName}.${ste.getMethodName}").getOrElse("")
  println(s"--------======== Starting Embedded Cassandra on port ${baseConfiguration.nativeTransportPort} ========--------\n$location")

  private[embedded] val process = new ProcessBuilder()
    .command(javaBin,
      "-Xms512M", "-Xmx1G", "-Xmn384M", "-XX:+UseConcMarkSweepGC",
      sizeEstimatesUpdateIntervalProperty,
      cassandraConfProperty, jammAgentProperty, superuserSetupDelayProperty, jmxPortProperty,
      logConfigFileProperty, "-cp", classPath, cassandraMainClass, "-f")
    .inheritIO()
    .start()

  val startupTime = System.currentTimeMillis()

  if (!waitForPortOpen(InetAddress.getByName(baseConfiguration.rpcAddress), baseConfiguration.nativeTransportPort, 100000, () => !process.isAlive)) {
    if (!process.isAlive) {
      System.err.println(s"!!! Cassandra at ${baseConfiguration.nativeTransportPort} is already stopped with exit code: ${process.exitValue()}")
    }
    throw new IOException(s"Failed to start Cassandra at ${baseConfiguration.rpcAddress}:${baseConfiguration.nativeTransportPort}")
  }

  def destroy() {
    System.err.println(s"========-------- Stopping Embedded Cassandra at ${baseConfiguration.nativeTransportPort} --------========")
    if (!process.isAlive) {
      System.err.println(s"!!! Cassandra at ${baseConfiguration.nativeTransportPort} is already stopped with exit code: ${process.exitValue()}")
    }
    process.destroy()
    process.waitFor()
    FileUtils.forceDelete(tempDir)
    tempDir.delete()
  }

  def nodeToolCmd(params: String*): Unit = {
    Try {
      val cmd = List(javaBin, "-Xms128M", "-Xmx512M", cassandraConfProperty, "-cp", classPath,
        nodeToolMainClass, "-h", baseConfiguration.listenAddress, "-p", baseConfiguration.jmxPort.toString) ++ params
      val nodeToolCmdProcess = new ProcessBuilder()
        .command(cmd: _*)
        .inheritIO()
        .start()
      nodeToolCmdProcess.waitFor()
      nodeToolCmdProcess.destroy()
    }
  }
}

object CassandraRunner {
  val SizeEstimatesUpdateIntervalInSeconds = 5
  val DefaultJmxPort = 7199
}
