package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{SocketOptions, AuthProvider, Cluster, PlainTextAuthProvider}
import com.datastax.spark.connector.util.ReflectionUtil
import org.apache.cassandra.thrift.{AuthenticationRequest, Cassandra, TFramedTransportFactory}
import org.apache.spark.SparkConf
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TTransport

import scala.collection.JavaConversions._

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory.class`
  * option. */
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress): (Cassandra.Iface, TTransport)

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object DefaultConnectionFactory extends CassandraConnectionFactory {

  val minReconnectionDelay = System.getProperty("spark.cassandra.connection.reconnection_delay_ms.min", "1000").toInt
  val maxReconnectionDelay = System.getProperty("spark.cassandra.connection.reconnection_delay_ms.max", "60000").toInt
  val localDC = System.getProperty("spark.cassandra.connection.local_dc")
  val retryCount = System.getProperty("spark.cassandra.query.retry.count", "10").toInt
  val connectTimeout = System.getProperty("spark.cassandra.connection.timeout_ms", "5000").toInt
  val readTimeout = System.getProperty("spark.cassandra.read.timeout_ms", "12000").toInt
  
  
  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  override def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress) = {
    val transportFactory = new TFramedTransportFactory
    val transport = transportFactory.openTransport(hostAddress.getHostAddress, conf.rpcPort)
    val client = new Cassandra.Client(new TBinaryProtocol(transport))
    conf.authConf.configureThriftClient(client)
    (client, transport)
  }

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(connectTimeout)
      .setReadTimeoutMillis(readTimeout)

    Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.nativePort)
      .withRetryPolicy(new MultipleRetryPolicy(retryCount))
      .withReconnectionPolicy(new ExponentialReconnectionPolicy(minReconnectionDelay, maxReconnectionDelay))
      .withLoadBalancingPolicy(new LocalNodeFirstLoadBalancingPolicy(conf.hosts, Option(localDC)))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
  }

  /** Creates and configures native Cassandra connection */
  override def createCluster(conf: CassandraConnectorConf): Cluster =
    clusterBuilder(conf).build()

}

/** Entry point for obtaining `CassandraConnectionFactory` object from `SparkConf`,
  * used when establishing connections to Cassandra. */
object CassandraConnectionFactory {
  val ConnectionFactoryProperty = "spark.cassandra.connection.factory.class"

  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    conf.getOption(ConnectionFactoryProperty)
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(DefaultConnectionFactory)
  }
}
