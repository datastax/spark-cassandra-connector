package com.datastax.driver.spark.util

import java.io.{FileOutputStream, IOException, File}
import java.net.InetAddress

import com.datastax.driver.spark.connector.CassandraConnector
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

  def useCassandraConfig(configTemplate: String) {
    if (currentConfigTemplate != configTemplate) {
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
