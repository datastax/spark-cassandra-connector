package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkConf
import scala.util.control.NonFatal

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  hosts: Set[InetAddress],
  nativePort: Int,
  rpcPort: Int,
  configurator: ConnectionConfigurator)

/** A factory for `CassandraConnectorConf` objects.
  * Allows for manually setting connection properties or reading them from `SparkConf` object.
  * By embedding connection information in `SparkConf`, `SparkContext` can offer Cassandra specific methods
  * which require establishing connections to a Cassandra cluster.*/
object CassandraConnectorConf extends Logging {

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042
  
  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "spark.cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "spark.cassandra.connection.native.port"


  def apply(host: InetAddress,
            nativePort: Int = DefaultNativePort,
            rpcPort: Int = DefaultRpcPort,
            configurator: ConnectionConfigurator = NoAuthConfigurator): CassandraConnectorConf = {
    CassandraConnectorConf(Set(host), nativePort, rpcPort, configurator)
  }

  def apply(conf: SparkConf): CassandraConnectorConf = {
    val hosts = conf.get(CassandraConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
      .split(",").flatMap { host =>
        try Some(InetAddress.getByName(host)) catch {
          case NonFatal(e) =>
            logError(s"Unknown host '$host'", e)
            None
        }
      }.toSet
    val rpcPort = conf.getInt(CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(CassandraConnectionNativePortProperty, DefaultNativePort)
    val configurator = ConnectionConfigurator.fromSparkConf(conf)
    CassandraConnectorConf(hosts, nativePort, rpcPort, configurator)
  }
  
}
