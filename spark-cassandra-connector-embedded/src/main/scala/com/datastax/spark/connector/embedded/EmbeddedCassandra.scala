package com.datastax.spark.connector.embedded

import java.net.InetAddress


/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server.*/
trait EmbeddedCassandra {


  /** Implementation hook. */
  def clearCache(): Unit

  /** Switches the Cassandra server to use the new configuration if the requested configuration is different
    * than the currently used configuration. When the configuration is switched, all the state (including data) of
    * the previously running cassandra cluster is lost.
    * @param configTemplates name of the cassandra.yaml template resources
    * @param forceReload if set to true, the server will be reloaded fresh even if the configuration didn't change */
  def useCassandraConfig(configTemplates: Seq[String], forceReload: Boolean = false) {
    import EmbeddedCassandra._
    if (!hosts.isEmpty && configTemplates.size > hosts.size) {
      throw new IllegalArgumentException("Configuration templates can't be more than the number of specified hosts")
    }
    if (sys.env.get(HostProperty).isEmpty) {
      clearCache()

      for (i <- configTemplates.indices) {
        if (configTemplates(i) == null || configTemplates(i).trim.isEmpty) {
          throw new IllegalArgumentException("Configuration template can't be null or empty")
        }
        if (currentConfigTemplates.lift(i) != Option(configTemplates(i)) || forceReload) {
          cassandras.lift(i).map(_.map(_.destroy()))
          cassandras = cassandras.patch(i, Seq(Some(new CassandraRunner(configTemplates(i), getProps(i)))), 1)
          currentConfigTemplates = currentConfigTemplates.patch(i, Seq(configTemplates(i)), 1)
        }
      }
    }
  }
}


object EmbeddedCassandra {

  val HostProperty = "IT_TEST_CASSANDRA_HOSTS"
  val NativePortProperty = "IT_TEST_CASSANDRA_NATIVE_PORTS"
  val RpcPortProperty = "IT_TEST_CASSANDRA_RPC_PORTS"

  validate

  private def validate() = {
    val hosts = sys.env.get(HostProperty)
    if (hosts != None) {
      val nativePorts = sys.env.get(NativePortProperty)
      if (nativePorts != None) {
        val rpcPorts = sys.env.get(RpcPortProperty)
        if (rpcPorts != None) {
          val hostSize = hosts.getOrElse("").split(",").size
          val nativePortSize = nativePorts.getOrElse("").split(",").size
          if (hostSize != nativePortSize) {
            throw new RuntimeException("IT_TEST_CASSANDRA_HOSTS must have same size as IT_TEST_CASSANDRA_NATIVE_PORTS")
          }
          val rpcPortSize = rpcPorts.getOrElse("").split(",").size
          if (hostSize != rpcPortSize) {
            throw new RuntimeException("IT_TEST_CASSANDRA_HOSTS must have same size as IT_TEST_CASSANDRA_RPC_PORTS")
          }
        } else {
          throw new RuntimeException("Missing IT_TEST_CASSANDRA_RPC_PORTS settings in system environment")
        }
      } else {
        throw new RuntimeException("Missing IT_TEST_CASSANDRA_NATIVE_PORTS settings in system environment")
      }
    }
  }

  val hosts: IndexedSeq[InetAddress] = {
    sys.env.get(HostProperty) match {
      case Some(h) => h.split(",").map(host => InetAddress.getByName(host.trim)).toIndexedSeq
      case None => IndexedSeq()
    }
  }

  val nativePorts: IndexedSeq[Int] = {
    sys.env.get(HostProperty) match {
      case Some(p) => p.split(",").map(port => port.trim.toInt).toIndexedSeq
      case None => IndexedSeq()
    }
  }

  val rpcPorts: IndexedSeq[Int] = {
    sys.env.get(HostProperty) match {
      case Some(p) => p.split(",").map(port => port.trim.toInt).toIndexedSeq
      case None => IndexedSeq()
    }
  }

  private[connector] var cassandras: IndexedSeq[Option[CassandraRunner]] = IndexedSeq(None)

  private[connector] var currentConfigTemplates: IndexedSeq[String] = IndexedSeq("")

  def getProps(index: Integer): Map[String, String] = {
    if (hosts.isEmpty || index < hosts.size) {
      val host = if (hosts.isEmpty) "127.0.0.1" else hosts(index).getHostAddress
      Map("seeds"               -> host,
        "storage_port"          -> s"700$index",
        "ssl_storage_port"      -> s"700${index + 1}",
        "native_transport_port" -> s"904${index + 2}",
        "rpc_address"           -> host,
        "rpc_port"              -> s"916$index",
        "listen_address"        -> host,
        "cluster_name"          -> s"Test Cluster$index")
    } else {
      throw new IllegalArgumentException(s"$index index is overflow the size of ${hosts.size}")
    }
  }

  def getHost(index: Integer): InetAddress = {
    if (hosts.isEmpty) {
      InetAddress.getByName("127.0.0.1")
    } else if (index < hosts.size) {
      hosts(index)
    } else {
      throw new RuntimeException(s"$index index is overflow the size of ${hosts.size}")
    }
  }

  def getNativePort(index: Integer): Integer = {
    if (nativePorts.isEmpty) {
      9042 + index
    } else if (index < hosts.size) {
      nativePorts(index)
    } else {
      throw new RuntimeException(s"$index index is overflow the size of ${hosts.size}")
    }
  }

  def getRpcPort(index: Integer): Integer = {
    if (nativePorts.isEmpty) {
      9160 + index
    } else if (index < hosts.size) {
      rpcPorts(index)
    } else {
      throw new RuntimeException(s"$index index is overflow the size of ${hosts.size}")
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() = cassandras.map(_.map(_.destroy()))
  }))
}

private[connector] class CassandraRunner(val configTemplate: String, props: Map[String, String]) extends Embedded {

  import java.io.{File, FileOutputStream, IOException}
  import org.apache.cassandra.io.util.FileUtils
  import com.google.common.io.Files

  val tempDir = mkdir(new File(Files.createTempDir(), "cassandra-driver-spark"))
  val workDir = mkdir(new File(tempDir, "cassandra"))
  val dataDir = mkdir(new File(workDir, "data"))
  val commitLogDir = mkdir(new File(workDir, "commitlog"))
  val cachesDir = mkdir(new File(workDir, "saved_caches"))
  val confDir = mkdir(new File(tempDir, "conf"))
  val confFile = new File(confDir, "cassandra.yaml")

  private val properties = if(props != null) Map("cassandra_dir" -> workDir.toString) ++ props else Map("cassandra_dir" -> workDir.toString)
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

  val nativePort =  props.get("native_transport_port").get.toInt
  if (!waitForPortOpen(InetAddress.getByName(props.get("rpc_address").get), nativePort, 100000))
    throw new IOException("Failed to start Cassandra.")

  def destroy() {
    process.destroy()
    process.waitFor()
    FileUtils.deleteRecursive(tempDir)
    tempDir.delete()
  }
}



