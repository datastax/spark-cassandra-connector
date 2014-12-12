package com.datastax.spark.connector.cql

import java.net.InetAddress

import org.apache.cassandra.thrift.{AuthenticationRequest, TFramedTransportFactory, Cassandra}
import org.apache.spark.SparkConf
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TTransport

import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{Cluster, SocketOptions}
import com.datastax.spark.connector.util.ReflectionUtil

import scala.collection.JavaConversions._

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory` option.*/
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress): (Cassandra.Iface, TTransport)

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object DefaultConnectionFactory extends CassandraConnectionFactory {

  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  override def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress) = {
    var transport: TTransport = null
    try {
      val transportFactory = new TFramedTransportFactory()
      transport = transportFactory.openTransport(hostAddress.getHostAddress, conf.rpcPort)
      val client = new Cassandra.Client(new TBinaryProtocol(transport))
      val creds = conf.authConf.thriftCredentials
      if (creds.nonEmpty) {
        client.login(new AuthenticationRequest(creds))
      }
      (client, transport)
    }
    catch {
      case e: Throwable =>
        if (transport != null)
          transport.close()
        throw e
    }
  }

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectionOptions.connectTimeout)
      .setReadTimeoutMillis(conf.connectionOptions.readTimeout)

    Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.nativePort)
      .withRetryPolicy(new MultipleRetryPolicy(conf.connectionOptions.retryCount))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(
          conf.connectionOptions.minReconnectionDelay, conf.connectionOptions.maxReconnectionDelay))
      .withLoadBalancingPolicy(
        new LocalNodeFirstLoadBalancingPolicy(conf.hosts, Option(conf.connectionOptions.localDC)))
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
  val ConnectionFactoryProperty = "spark.cassandra.connection.factory"

  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    conf.getOption(ConnectionFactoryProperty)
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(DefaultConnectionFactory)
  }
}
