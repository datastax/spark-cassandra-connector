package com.datastax.spark.connector.util

import java.io.{FileOutputStream, IOException, File}
import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnector
import com.google.common.io.Files
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.service.CassandraDaemon
import org.apache.cassandra.thrift.Cassandra

/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server.*/
trait CassandraServer {
  def useCassandraConfig(configTemplate: String) {
    CassandraServer.useCassandraConfig(configTemplate)
  }
}

object CassandraServer {

  private var cassandra: CassandraServerRunner = null
  private var currentConfigTemplate: String = null

  /** Switches the Cassandra server to use the new configuration if the requested configuration is different
    * than the currently used configuration. When the configuration is switched, all the state (including data) of
    * the previously running cassandra cluster is lost.
    * @param configTemplate name of the cassandra.yaml template resource
    * @param forceReload if set to true, the server will be reloaded fresh even if the configuration didn't change */
  def useCassandraConfig(configTemplate: String, forceReload: Boolean = false) {
    if (currentConfigTemplate != configTemplate || forceReload) {
      if (cassandra != null)
        cassandra.destroy()

      CassandraConnector.evictCache()
      cassandra = new CassandraServerRunner(configTemplate)

      currentConfigTemplate = currentConfigTemplate
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() =
      if (cassandra != null)
        cassandra.destroy()
  }))

}
