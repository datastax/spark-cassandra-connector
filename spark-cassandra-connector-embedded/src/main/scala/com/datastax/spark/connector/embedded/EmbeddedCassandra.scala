package com.datastax.spark.connector.embedded

import java.net.InetAddress

import org.apache.commons.configuration.ConfigurationException


/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server.*/
trait EmbeddedCassandra {

  /** Implementation hook. */
  def clearCache(): Unit

  /** Switches the Cassandra server to use the new configuration if the requested configuration
    * is different than the currently used configuration. When the configuration is switched, all
    * the state (including data) of the previously running cassandra cluster is lost.
    * @param configTemplates name of the cassandra.yaml template resources
    * @param forceReload if set to true, the server will be reloaded fresh
    *   even if the configuration didn't change */
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
          cassandraRunners.lift(i).flatten.foreach(_.destroy())
          cassandraRunners = cassandraRunners.patch(i,
            Seq(Some(new CassandraRunner(configTemplates(i), getProps(i)))), 1)
          currentConfigTemplates = currentConfigTemplates.patch(i, Seq(configTemplates(i)), 1)
        }
      }
    }
  }
}

object UserDefinedProperty {

  trait TypedProperty {
    type ValueType
    def convertValueFromString(str: String): ValueType
    def checkValueType(obj: Any): ValueType
  }

  trait IntProperty extends TypedProperty {
    type ValueType = Int
    def convertValueFromString(str: String) = str.toInt
    def checkValueType(obj: Any) =
      obj match {
        case x: Int => x
        case _ => throw new ClassCastException (s"Expected Int but found ${obj.getClass.getName}")
      }
  }

  trait InetAddressProperty extends TypedProperty {
    type ValueType = InetAddress
    def convertValueFromString(str: String) = InetAddress.getByName(str)
    def checkValueType(obj: Any) =
      obj match {
        case x: InetAddress => x
        case _ => throw new ClassCastException (s"Expected InetAddress but found ${obj.getClass.getName}")
      }
  }

  abstract sealed class NodeProperty(val propertyName: String) extends TypedProperty
  case object HostProperty extends NodeProperty("IT_TEST_CASSANDRA_HOSTS") with InetAddressProperty
  case object PortProperty extends NodeProperty("IT_TEST_CASSANDRA_PORTS") with IntProperty

  private def getValueSeq(propertyName: String): Seq[String] = {
    sys.env.get(propertyName) match {
      case Some(p) => p.split(",").map(e => e.trim).toIndexedSeq
      case None => IndexedSeq()
    }
  }

  private def getValueSeq(nodeProperty: NodeProperty): Seq[nodeProperty.ValueType] =
    getValueSeq(nodeProperty.propertyName).map(x => nodeProperty.convertValueFromString(x))

  val hosts = getValueSeq(HostProperty)
  val ports = getValueSeq(PortProperty)

  def getProperty(nodeProperty: NodeProperty): Option[String] =
    sys.env.get(nodeProperty.propertyName)
  
  def getPropertyOrThrowIfNotFound(nodeProperty: NodeProperty): String =
    getProperty(nodeProperty).getOrElse(
      throw new ConfigurationException(s"Missing ${nodeProperty.propertyName} in system environment"))
}

object EmbeddedCassandra {

  import UserDefinedProperty._
  
  private def countCommaSeparatedItemsIn(s: String): Int = 
    s.count(_ == ',')
  
  getProperty(HostProperty) match {
    case None =>
    case Some(hostsStr) =>
      val hostCount = countCommaSeparatedItemsIn(hostsStr)

      val nativePortsStr = getPropertyOrThrowIfNotFound(PortProperty)
      val nativePortCount = countCommaSeparatedItemsIn(nativePortsStr)
      require(hostCount == nativePortCount,
        "IT_TEST_CASSANDRA_HOSTS must have the same size as IT_TEST_CASSANDRA_NATIVE_PORTS")
  }

  private[connector] var cassandraRunners: IndexedSeq[Option[CassandraRunner]] = IndexedSeq(None)

  private[connector] var currentConfigTemplates: IndexedSeq[String] = IndexedSeq()

  def getProps(index: Integer): Map[String, String] = {
    require(hosts.isEmpty || index < hosts.length, s"$index index is overflow the size of ${hosts.length}")
    val host = getHost(index).getHostAddress
    Map("seeds"               -> host,
      "storage_port"          -> getStoragePort(index).toString,
      "ssl_storage_port"      -> getSslStoragePort(index).toString,
      "native_transport_port" -> getPort(index).toString,
      "rpc_address"           -> host,
      "listen_address"        -> host,
      "cluster_name"          -> getClusterName(index))
  }

  def getStoragePort(index: Integer) = 7000 + index
  def getSslStoragePort(index: Integer) = 7100 + index
  def getClusterName(index: Integer) = s"Test Cluster$index"

  def getHost(index: Integer): InetAddress = getNodeProperty(index, HostProperty)
  def getPort(index: Integer) = getNodeProperty(index, PortProperty)

  private def getNodeProperty(index: Integer, nodeProperty: NodeProperty): nodeProperty.ValueType = {
    nodeProperty.checkValueType {
      nodeProperty match {
        case PortProperty if ports.isEmpty => 9042 + index
        case PortProperty if index < hosts.size => ports(index)
        case HostProperty if hosts.isEmpty => InetAddress.getByName("127.0.0.1")
        case HostProperty if index < hosts.size => hosts(index)
        case _ => throw new RuntimeException(s"$index index is overflow the size of ${hosts.size}")
      }
    }
  }   

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() = cassandraRunners.flatten.foreach(_.destroy())
  }))
}

private[connector] class CassandraRunner(val configTemplate: String, props: Map[String, String])
  extends Embedded {

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



