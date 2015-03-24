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
    import UserDefinedProperty._
    require(hosts.isEmpty || configTemplates.size <= hosts.size,
      "Configuration templates can't be more than the number of specified hosts")

    if (getProperty(HostProperty).isEmpty) {
      clearCache()

      val templatePairs = configTemplates.zipAll(currentConfigTemplates, "missing value", null)
      for (i <- configTemplates.indices) {
        require(configTemplates(i) != null && configTemplates(i).trim.nonEmpty,
          "Configuration template can't be null or empty")

        if (templatePairs(i)._2 != templatePairs(i)._1 || forceReload) {
          cassandraRunners.lift(i).map(_.map(_.destroy()))
          cassandraRunners = cassandraRunners.patch(i, Seq(Some(new CassandraRunner(configTemplates(i), getProps(i)))), 1)
          currentConfigTemplates = currentConfigTemplates.patch(i, Seq(configTemplates(i)), 1)
        }
      }
    }
  }
}

object UserDefinedProperty {
  abstract class NodeProperty(val propertyName: String)
  type PropertyType = NodeProperty {val propertyName: String}
  case object HostProperty extends NodeProperty("IT_TEST_CASSANDRA_HOSTS")
  case object NativePortProperty extends NodeProperty("IT_TEST_CASSANDRA_NATIVE_PORTS")
  case object RpcPortProperty extends NodeProperty("IT_TEST_CASSANDRA_RPC_PORTS")

  val hosts = getNodePropertySeq(HostProperty).asInstanceOf[IndexedSeq[InetAddress]]
  val nativePorts = getNodePropertySeq(NativePortProperty).asInstanceOf[IndexedSeq[Int]]
  val rpcPorts = getNodePropertySeq(RpcPortProperty).asInstanceOf[IndexedSeq[Int]]

  private def getNodePropertySeq(nodeProperty: NodeProperty): IndexedSeq[Any] = {
    nodeProperty match {
      case NativePortProperty => getValueSeq(NativePortProperty.propertyName).map(e => e.toInt)
      case RpcPortProperty => getValueSeq(RpcPortProperty.propertyName).map(e => e.toInt)
      case HostProperty => getValueSeq(HostProperty.propertyName).map(e => InetAddress.getByName(e))
      case _ => throw new RuntimeException("Wrong node input property")
    }
  }

  private def getValueSeq(propertyName: String) = {
    sys.env.get(propertyName) match {
      case Some(p) => p.split(",").map(e => e.trim).toIndexedSeq
      case None => IndexedSeq()
    }
  }

  def getProperty(nodeProperty: NodeProperty) = {
    nodeProperty match {
      case p: PropertyType => sys.env.get(p.propertyName)
      case _ => throw new RuntimeException("Wrong node input property")
    }
  }
}

object EmbeddedCassandra {

  import UserDefinedProperty._

  getProperty(HostProperty) match {
    case Some(h) =>
      val hostSize = h.split(",").size
      getProperty(NativePortProperty) match {
        case Some(np) =>
          val nativePortSize = np.split(",").size
          require(hostSize == nativePortSize, "IT_TEST_CASSANDRA_HOSTS must have same size as IT_TEST_CASSANDRA_NATIVE_PORTS")
          getProperty(RpcPortProperty) match {
            case Some(rp) =>
              val rpcPortSize = rp.split(",").size
              require(hostSize == rpcPortSize, "IT_TEST_CASSANDRA_HOSTS must have same size as IT_TEST_CASSANDRA_RPC_PORTS")
            case None => throw new RuntimeException("Missing IT_TEST_CASSANDRA_RPC_PORTS settings in system environment")
          }
        case None => throw new RuntimeException("Missing IT_TEST_CASSANDRA_NATIVE_PORTS settings in system environment")
      }
    case None =>
  }

  private[connector] var cassandraRunners: IndexedSeq[Option[CassandraRunner]] = IndexedSeq(None)

  private[connector] var currentConfigTemplates: IndexedSeq[String] = IndexedSeq()

  def getProps(index: Integer): Map[String, String] = {
    require(hosts.isEmpty || index < hosts.size, s"$index index is overflow the size of ${hosts.size}")
    val host = getHost(index).getHostAddress
    Map("seeds"               -> host,
      "storage_port"          -> getStoragePort(index).toString,
      "ssl_storage_port"      -> getSslStoragePort(index).toString,
      "native_transport_port" -> getNativePort(index).toString,
      "rpc_address"           -> host,
      "rpc_port"              -> getRpcPort(index).toString,
      "listen_address"        -> host,
      "cluster_name"          -> getClusterName(index))
  }

  def getStoragePort(index: Integer) = 7000 + index
  def getSslStoragePort(index: Integer) = 7100 + index
  def getClusterName(index: Integer) = s"Test Cluster$index"

  def getHost(index: Integer) = getNodeProperty(index, HostProperty) match {
    case host: InetAddress => host
    case _ => throw new RuntimeException("Wrong data type, it should be InetAddress")
  }
  def getNativePort(index: Integer) = getNodeProperty(index, NativePortProperty) match {
    case port: Int => port
    case _ => throw new RuntimeException("Wrong data type, it should be Int")
  }
  def getRpcPort(index: Integer) = getNodeProperty(index, RpcPortProperty) match {
    case port: Int => port
    case _ => throw new RuntimeException("Wrong data type, it should be Int")
  }

  private def getNodeProperty(index: Integer, nodeProperty: NodeProperty): Any = {
    nodeProperty match {
      case NativePortProperty if (nativePorts.isEmpty) => 9042 + index
      case NativePortProperty if (index < hosts.size) => nativePorts(index)
      case RpcPortProperty if (rpcPorts.isEmpty) => 9160 + index
      case RpcPortProperty if (index < hosts.size) => rpcPorts(index)
      case HostProperty if (hosts.isEmpty) => InetAddress.getByName("127.0.0.1")
      case HostProperty if (index < hosts.size) => hosts(index)
      case _ => throw new RuntimeException(s"$index index is overflow the size of ${hosts.size}")
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() = cassandraRunners.map(_.map(_.destroy()))
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

  assert(props != null)
  private val properties = Map("cassandra_dir" -> workDir.toString) ++ props
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



