package com.datastax.spark.connector.embedded

import java.net.InetAddress

import com.datastax.spark.connector.embedded.YamlTransformations.CassandraConfiguration

/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server. */
trait EmbeddedCassandra {

  /** Implementation hook. */
  def clearCache(): Unit

  /** Switches the Cassandra server to use the new configuration if the requested configuration
    * is different than the currently used configuration. When the configuration is switched, all
    * the state (including data) of the previously running cassandra cluster is lost.
    *
    * @param configTemplates name of the cassandra.yaml template resources
    * @param forceReload if set to true, the server will be reloaded fresh
    *                    even if the configuration didn't change */
  def useCassandraConfig(configTemplates: Seq[YamlTransformations], forceReload: Boolean = false) {
    import com.datastax.spark.connector.embedded.EmbeddedCassandra._
    import com.datastax.spark.connector.embedded.UserDefinedProperty._
    require(hosts.isEmpty || configTemplates.size <= hosts.size,
      "Configuration templates can't be more than the number of specified hosts")

    if (getProperty(HostProperty).isEmpty) {
      clearCache()

      val templatePairs = configTemplates.zipAll(currentConfigTemplates, null, null)
      for (runnerToDestroy <- cassandraRunners.indices.toSet -- configTemplates.indices.toSet) {
        cassandraRunners.lift(runnerToDestroy).flatten.foreach(_.destroy())
        cassandraRunners = cassandraRunners.patch(runnerToDestroy, Seq(None), 1)
      }
      currentConfigTemplates = currentConfigTemplates.take(configTemplates.length)
      for (i <- configTemplates.indices) {
        require(configTemplates(i) != null, "Configuration template can't be null or empty")

        if (templatePairs(i)._2 != templatePairs(i)._1 || forceReload || templatePairs(i)._2 == null) {
          cassandraRunners.lift(i).flatten.foreach(_.destroy())
          cassandraRunners = cassandraRunners.patch(i,
            Seq(Some(new CassandraRunner(configTemplates(i), getBaseYamlTransformer(i)))), 1)
          currentConfigTemplates = currentConfigTemplates.patch(i, Seq(configTemplates(i)), 1)
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

  private[connector] var currentConfigTemplates: IndexedSeq[YamlTransformations] = IndexedSeq()

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

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() = {
      cassandraRunners.flatten.foreach(_.destroy())
      release()
    }
  }))

}
