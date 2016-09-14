package com.datastax.spark.connector.embedded

import java.net.InetAddress

import com.datastax.spark.connector.embedded.YamlTransformations.CassandraConfiguration

import scala.collection.mutable

/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server. */
trait EmbeddedCassandra {

  import EmbeddedCassandra._

  /** Implementation hook. */
  def clearCache(): Unit

  def versionGreaterThanOrEquals(major: Int, minor: Int = 0): Boolean = {
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
  def useCassandraConfig(configTemplates: Seq[YamlTransformations], forceReload: Boolean = false) {
    import com.datastax.spark.connector.embedded.UserDefinedProperty._

    require(hosts.isEmpty || configTemplates.size <= hosts.size,
      "Configuration templates can't be more than the number of specified hosts")
    require(!configTemplates.contains(null), "Configuration template cannot be null")

    if (getProperty(HostProperty).isEmpty) {
      clearCache()

      val providedConfTmplts = configTemplates.zipWithIndex.map(_.swap).toMap

      // destroy the currently running nodes if: forceReload, template was not provided
      // or template does not match the currently used one
      for (id <- cassandraRunners.keySet.toList) {
        val tmpltProvided = providedConfTmplts.contains(id)
        val providedTmpltMatchesCur = providedConfTmplts.get(id) == currentConfigTemplates.get(id)
        if (forceReload || !tmpltProvided || !providedTmpltMatchesCur) {
          destroyAndRemoveRunner(id)
        }
      }

      // start nodes for provided template which are not already runnning
      for ((id, tmplt) <- (providedConfTmplts -- cassandraRunners.keySet).toList) {
        createAndAddRunner(id, tmplt)
      }
    }
  }

  private def destroyAndRemoveRunner(id: Int): Unit = {
    cassandraRunners.get(id).foreach(_.destroy())
    cassandraRunners -= id
    currentConfigTemplates -= id
  }

  private def createAndAddRunner(id: Int, configTemplate: YamlTransformations): Unit = {
    cassandraRunners += id -> new CassandraRunner(configTemplate, getBaseYamlTransformer(id))
    currentConfigTemplates += id -> configTemplate
  }

}


object EmbeddedCassandra {

  import com.datastax.spark.connector.embedded.UserDefinedProperty._

  val DEFAULT_CASSANDRA_VERSION = "3.0.8"
  val cassandraVersion = System.getProperty("test.cassandra.version", DEFAULT_CASSANDRA_VERSION)
  val (cassandraMajorVersion, cassandraMinorVersion) = {
    val parts = cassandraVersion.split("\\.")
    require(parts.length > 2, s"Can't determine Cassandra Version from $cassandraVersion : ${parts.mkString(",")}")
    (parts(0).toInt, parts(1).toInt)
  }

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

  private[connector] var cassandraRunners = mutable.HashMap[Int, CassandraRunner]()

  private[connector] var currentConfigTemplates = mutable.HashMap[Int, YamlTransformations]()

  private def countCommaSeparatedItemsIn(s: String): Int = s.count(_ == ',')

  def getBaseYamlTransformer(index: Integer): CassandraConfiguration = {
    require(hosts.isEmpty || index < hosts.length, s"$index index is overflow the size of ${hosts.length}")
    val host = getHost(index).getHostAddress
    YamlTransformations.CassandraConfiguration(
      seeds = List.empty,
      clusterName = getClusterName(index),
      storagePort = cassandraPorts.getStoragePort(index),
      sslStoragePort = cassandraPorts.getSslStoragePort(index),
      nativeTransportPort = getPort(index),
      rpcAddress = host,
      listenAddress = host,
      jmxPort = cassandraPorts.getJmxPort(index)
    )
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
    cassandraRunners.values.foreach(_.destroy())
    release()
  }

  private[connector] def removeShutdownHook(): Boolean = {
    Runtime.getRuntime.removeShutdownHook(shutdownThread)
  }
}
