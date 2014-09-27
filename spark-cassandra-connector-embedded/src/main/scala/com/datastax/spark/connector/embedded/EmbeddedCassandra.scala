package com.datastax.spark.connector.embedded

import java.net.InetAddress

/** A utility trait for integration testing and quick prototyping or demos.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server.
  *
  * Shutdown hook is called automatically.
  */
trait EmbeddedCassandra {

  def cassandraHost = EmbeddedCassandra.cassandraHost

  /** Implementation hook. */
  def clearCache(): Unit

  def startCassandra(configTemplate: String = "cassandra-default.yaml.template"): Unit =
    useCassandraConfig(configTemplate)

  /** Switches the Cassandra server to use the new configuration if the requested configuration is different
    * than the currently used configuration. When the configuration is switched, all the state (including data) of
    * the previously running cassandra cluster is lost.
    * @param configTemplate name of the cassandra.yaml template resource
    * @param forceReload if set to true, the server will be reloaded fresh even if the configuration didn't change */
  def useCassandraConfig(configTemplate: String, forceReload: Boolean = false) {
    import EmbeddedCassandra._
    if (sys.env.get(HostProperty).isEmpty) {
      if (currentConfigTemplate != configTemplate || forceReload) {
        clearCache()
        cassandra.map(_.destroy())
        cassandra = Some(new CassandraRunner(configTemplate))
        currentConfigTemplate = configTemplate
      }
    }
  }
}

object EmbeddedCassandra {

  val DefaultHost = "127.0.0.1"

  val HostProperty = "CASSANDRA_HOST"

  private[connector] var cassandra: Option[CassandraRunner] = None

  private[connector] var currentConfigTemplate: String = null

  val cassandraHost = InetAddress.getByName(sys.env.getOrElse(HostProperty, DefaultHost))

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() = cassandra.map(_.destroy())
  }))

}

private[connector] class CassandraRunner(val configTemplate: String) extends Embedded {

  import java.io.{File, FileOutputStream, IOException}
  import org.apache.cassandra.io.util.FileUtils
  import com.google.common.io.Files
  import EmbeddedCassandra._

  final val DefaultNativePort = 9042
  val tempDir = mkdir(new File(Files.createTempDir(), "cassandra-driver-spark"))
  val workDir = mkdir(new File(tempDir, "cassandra"))
  val dataDir = mkdir(new File(workDir, "data"))
  val commitLogDir = mkdir(new File(workDir, "commitlog"))
  val cachesDir = mkdir(new File(workDir, "saved_caches"))
  val confDir = mkdir(new File(tempDir, "conf"))
  val confFile = new File(confDir, "cassandra.yaml")

  private val properties = Map("cassandra_dir" -> workDir.toString)
  closeAfterUse(ClassLoader.getSystemResourceAsStream(configTemplate)) { input =>
    closeAfterUse(new FileOutputStream(confFile)) { output =>
      copyTextFileWithVariableSubstitution(input, output, properties)
    }
  }

  private val classPath = System.getProperty("java.class.path")
  private val javaBin = System.getProperty("java.home") + "/bin/java"
  private val cassandraConfProperty = "-Dcassandra.config=file:" + confFile.toString
  private val superuserSetupDelayProperty = "-Dcassandra.superuser_setup_delay_ms=0"
  private val jammAgent = classPath.split(File.pathSeparator).find(_.matches(".*jamm.*\\.jar"))
  private val jammAgentProperty = jammAgent.map("-javaagent:" + _).getOrElse("")
  private val cassandraMainClass = "org.apache.cassandra.service.CassandraDaemon"

  private val process = new ProcessBuilder()
    .command(javaBin,
      "-Xms2G", "-Xmx2G", "-Xmn384M", "-XX:+UseConcMarkSweepGC",
      cassandraConfProperty, jammAgentProperty, superuserSetupDelayProperty, "-cp", classPath,
      cassandraMainClass, "-f")
    .inheritIO()
    .start()

  if (!waitForPortOpen(InetAddress.getByName(DefaultHost), DefaultNativePort, 10000))
    throw new IOException("Failed to start Cassandra.")

  def destroy() {
    process.destroy()
    process.waitFor()
    FileUtils.deleteRecursive(tempDir)
    tempDir.delete()
  }

}



