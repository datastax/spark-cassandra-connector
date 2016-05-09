package com.datastax.spark.connector.embedded

import java.net.InetAddress
import java.nio.file.Paths

/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server. */
trait EmbeddedCassandra {

  /** Implementation hook. */
  def clearCache(): Unit

  val DEFAULT_CASSANDRA_VERSION = "3.0.2"
  val cassandraVersion = System.getProperty("test.cassandra.version", DEFAULT_CASSANDRA_VERSION)
  val (cassandraMajorVersion, cassandraMinorVersion) = {
    val parts = cassandraVersion.split("\\.")
    require (parts.length > 2, s"Can't determine Cassandra Version from $cassandraVersion : ${parts.mkString(",")}")
    (parts(0).toInt, parts(1).toInt)
  }

  def versionGreaterThanOrEquals(major:Int, minor:Int = 0): Boolean = {
    (major < cassandraMajorVersion) ||
      (major == cassandraMajorVersion && minor <= cassandraMinorVersion)
  }


  /** Switches the Cassandra server to use the new configuration if the requested configuration
    * is different than the currently used configuration. When the configuration is switched, all
    * the state (including data) of the previously running cassandra cluster is lost.
    *
    * @param configTemplates name of the cassandra.yaml template resources
    * @param forceReload if set to true, the server will be reloaded fresh
    *                    even if the configuration didn't change */
  def useCassandraConfig(configTemplates: Seq[String], forceReload: Boolean = false) {
    import com.datastax.spark.connector.embedded.EmbeddedCassandra._
    import com.datastax.spark.connector.embedded.UserDefinedProperty._
    require(hosts.isEmpty || configTemplates.size <= hosts.size,
      "Configuration templates can't be more than the number of specified hosts")


    val templateDir = {
      if (cassandraMajorVersion >= 3) {
        cassandraMajorVersion.toString
      } else {
        s"$cassandraMajorVersion.$cassandraMinorVersion"
      }
    }

    val versionedConfigTemplates = configTemplates.map( templateFile =>
      Paths.get(templateDir, templateFile).toString)


    if (getProperty(HostProperty).isEmpty) {
      clearCache()

      val templatePairs = versionedConfigTemplates
        .zipAll(currentConfigTemplates, "missing value", null)

      for (i <- cassandraRunners.indices.toSet -- configTemplates.indices.toSet) {
        cassandraRunners.lift(i).flatten.foreach(_.destroy())
        cassandraRunners = cassandraRunners.patch(i, Seq(None), 1)
      }
      currentConfigTemplates = currentConfigTemplates.take(configTemplates.length)
      for (i <- configTemplates.indices) {
        require(configTemplates(i) != null && configTemplates(i).trim.nonEmpty,
          "Configuration template can't be null or empty")

        if (templatePairs(i)._2 != templatePairs(i)._1 || forceReload) {
          cassandraRunners.lift(i).flatten.foreach(_.destroy())
          cassandraRunners = cassandraRunners.patch(i,
            Seq(Some(new CassandraRunner(versionedConfigTemplates(i), getProps(i)))), 1)
          currentConfigTemplates = currentConfigTemplates.patch(i, Seq(versionedConfigTemplates(i)), 1)
        }
      }
    }
  }
}


object EmbeddedCassandra {

  import com.datastax.spark.connector.embedded.UserDefinedProperty._

  getProperty(HostProperty) match {
    case None =>
    case Some(hostsStr) =>
      val hostCount = countCommaSeparatedItemsIn(hostsStr)

      val nativePortsStr = getPropertyOrThrowIfNotFound(PortProperty)
      val nativePortCount = countCommaSeparatedItemsIn(nativePortsStr)
      require(hostCount == nativePortCount,
        "IT_TEST_CASSANDRA_HOSTS must have the same size as IT_TEST_CASSANDRA_NATIVE_PORTS")
  }

  private val cassandraPorts: CassandraPorts = {
    if (hosts.nonEmpty || ports.nonEmpty) {
      CassandraPorts(ports)
    } else {
      DynamicCassandraPorts()
    }
  }

  private[connector] var cassandraRunners: IndexedSeq[Option[CassandraRunner]] = IndexedSeq(None)

  private[connector] var currentConfigTemplates: IndexedSeq[String] = IndexedSeq()

  private def countCommaSeparatedItemsIn(s: String): Int = s.count(_ == ',')

  def getProps(index: Integer): Map[String, String] = {
    require(hosts.isEmpty || index < hosts.length, s"$index index is overflow the size of ${hosts.length}")
    val host = getHost(index).getHostAddress
    Map(
      "seeds" -> "",
      "storage_port" -> cassandraPorts.getStoragePort(index).toString,
      "ssl_storage_port" -> cassandraPorts.getSslStoragePort(index).toString,
      "native_transport_port" -> getPort(index).toString,
      "jmx_port" -> cassandraPorts.getJmxPort(index).toString,
      "rpc_address" -> host,
      "listen_address" -> host,
      "cluster_name" -> getClusterName(index),
      "keystore_path" -> ClassLoader.getSystemResource("keystore").getPath)
  }

  def getClusterName(index: Integer) = s"Test Cluster $index"

  def getHost(index: Integer): InetAddress =
    if (hosts.isEmpty) InetAddress.getByName("127.0.0.1") else hosts(index)

  def getPort(index: Integer): Int = cassandraPorts.getRpcPort(index)

  def release(): Unit = {
    cassandraPorts match {
      case pr: DynamicCassandraPorts => pr.release()
      case _ =>
    }
  }

  private val shutdownThread: Thread = new Thread("Shutdown embedded C* hook thread") {
    override def run() = {
      shutdown()
    }
  }

  Runtime.getRuntime.addShutdownHook(shutdownThread)

  private[connector] def shutdown(): Unit = {
    cassandraRunners.flatten.foreach(_.destroy())
    release()
  }

  private[connector] def removeShutdownHook(): Boolean = {
    Runtime.getRuntime.removeShutdownHook(shutdownThread)
  }
}
