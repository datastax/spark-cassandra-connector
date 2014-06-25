package com.datastax.driver.spark.connector

import java.net.InetAddress
import org.apache.spark.SparkConf

case class AuthConfig(credentials: Option[(String, String)])

object AuthConfig {

  val CassandraUserNameProperty = "cassandra.username"
  val CassandraPasswordProperty = "cassandra.password"
  val HadoopAuthenticationProperty = "hadoop.security.authentication"

  def fromSparkConf(conf: SparkConf) = {
    val credentials =
      for (username <- conf.getOption(CassandraUserNameProperty);
           password <- conf.getOption(CassandraPasswordProperty)) yield (username, password)
    AuthConfig(credentials)
  }
}

case class CassandraConnectionConfig(hosts: Set[InetAddress], nativePort: Int, rpcPort: Int, authConfig: AuthConfig)

object CassandraConnectionConfig {

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042
  
  val CassandraConnectionHostProperty = "cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "cassandra.connection.native.port"


  def apply(host: InetAddress,
            nativePort: Int = DefaultNativePort,
            rpcPort: Int = DefaultRpcPort,
            authConfig: AuthConfig = AuthConfig(None)): CassandraConnectionConfig = {
    CassandraConnectionConfig(Set(host), nativePort, rpcPort, authConfig)
  }

  def apply(conf: SparkConf): CassandraConnectionConfig = {
    val host = InetAddress.getByName(conf.get(CassandraConnectionHostProperty, InetAddress.getLocalHost.getHostAddress))
    val rpcPort = conf.getInt(CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(CassandraConnectionNativePortProperty, DefaultNativePort)
    val authConfig = AuthConfig.fromSparkConf(conf)
    CassandraConnectionConfig(Set(host), nativePort, rpcPort, authConfig)
  }
  
}
