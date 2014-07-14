package com.datastax.spark.connector.cql

import java.net.InetAddress

import org.apache.spark.SparkConf

import com.datastax.driver.core.ConsistencyLevel

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  hosts: Set[InetAddress],
  nativePort: Int,
  rpcPort: Int,
  authConf: AuthConf,
  inputConsistencyLevel: ConsistencyLevel,
  outputConsistencyLevel: ConsistencyLevel)

/** A factory for `CassandraConnectorConf` objects.
  * Allows for manually setting connection properties or reading them from `SparkConf` object.
  * By embedding connection information in `SparkConf`, `SparkContext` can offer Cassandra specific methods
  * which require establishing connections to a Cassandra cluster.*/
object CassandraConnectorConf {

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042
  val DefaultInputConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultOutputConsistencyLevel = ConsistencyLevel.ONE
  
  val CassandraConnectionHostProperty = "cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "cassandra.connection.native.port"
  val CassandraInputConsistencyLevelProperty = "cassandra.input.consistency.level"
  val CassandraOutputConsistencyLevelProperty = "cassandra.output.consistency.level"


  def apply(host: InetAddress,
            nativePort: Int = DefaultNativePort,
            rpcPort: Int = DefaultRpcPort,
            authConf: AuthConf = NoAuthConf,
            inputConsistencyLevel: ConsistencyLevel = DefaultInputConsistencyLevel,
            outputConsistencyLevel: ConsistencyLevel = DefaultOutputConsistencyLevel): CassandraConnectorConf = {
    CassandraConnectorConf(Set(host), nativePort, rpcPort, authConf, inputConsistencyLevel, outputConsistencyLevel)
  }

  def apply(conf: SparkConf): CassandraConnectorConf = {
    val host = InetAddress.getByName(conf.get(CassandraConnectionHostProperty, InetAddress.getLocalHost.getHostAddress))
    val rpcPort = conf.getInt(CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(CassandraConnectionNativePortProperty, DefaultNativePort)
    val authConf = AuthConf.fromSparkConf(conf)
    val inputConsistencyLevel = ConsistencyLevel.valueOf(conf.get(CassandraInputConsistencyLevelProperty, DefaultInputConsistencyLevel.name))
    val outputConsistencyLevel = ConsistencyLevel.valueOf(conf.get(CassandraOutputConsistencyLevelProperty, DefaultOutputConsistencyLevel.name))
    CassandraConnectorConf(Set(host), nativePort, rpcPort, authConf, inputConsistencyLevel, outputConsistencyLevel)
  }
  
}
