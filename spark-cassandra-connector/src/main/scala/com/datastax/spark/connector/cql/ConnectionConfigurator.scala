package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{AuthProvider, Cluster, PlainTextAuthProvider}
import org.apache.cassandra.thrift.{AuthenticationRequest, Cassandra, TFramedTransportFactory}
import org.apache.spark.SparkConf
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TTransport

import scala.collection.JavaConversions._

/** Configures both native and Thrift connections to Cassandra.
  * This driver provides implementations [[NoAuthConfigurator]] for no authentication
  * and [[PasswordAuthConfigurator]] for password authentication. Other
  * configurators can be plugged in by setting `spark.cassandra.connection.conf.factory.class`
  * option. See [[ConnectionConfiguratorFactory]]. */
trait ConnectionConfigurator extends Serializable {

  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def createThriftClient(hostAddress: InetAddress, rpcPort: Int): (Cassandra.Iface, TTransport) = {
    val transportFactory = new TFramedTransportFactory
    val transport = transportFactory.openTransport(hostAddress.getHostAddress, rpcPort)
    val client = new Cassandra.Client(new TBinaryProtocol(transport))
    (client, transport)
  }

  /** Sets appropriate connection options for native connections. */
  def configureClusterBuilder(builder: Cluster.Builder): Cluster.Builder

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
case object NoAuthConfigurator extends ConnectionConfigurator {
  override def configureClusterBuilder(builder: Builder): Builder = {
    builder.withAuthProvider(AuthProvider.NONE)
  }
}

/** Performs plain-text password authentication. Use with `PasswordAuthenticator` in Cassandra. */
case class PasswordAuthConfigurator(user: String, password: String) extends ConnectionConfigurator {
  override def createThriftClient(hostAddress: InetAddress, rpcPort: Int) = {
    val (client, transport) = super.createThriftClient(hostAddress, rpcPort)
    val authRequest = new AuthenticationRequest(Map("username" -> user, "password" -> password))
    client.login(authRequest)
    (client, transport)
  }

  override def configureClusterBuilder(builder: Builder): Builder = {
    builder.withAuthProvider(new PlainTextAuthProvider(user, password))
  }
}

/** Obtains a connection configurator by reading  `SparkConf` object. */
trait ConnectionConfiguratorFactory {
  def configurator(conf: SparkConf): ConnectionConfigurator
}

/** Default `ConnectionConfiguratorFactory` that supports no authentication or password authentication.
  * Password authentication is enabled when both `spark.cassandra.auth.username` and `spark.cassandra.auth.password`
  * options are present in `SparkConf`. */
class DefaultConnConfFactory extends ConnectionConfiguratorFactory {

  val CassandraUserNameProperty = "spark.cassandra.auth.username"
  val CassandraPasswordProperty = "spark.cassandra.auth.password"

  def configurator(conf: SparkConf): ConnectionConfigurator = {
    val credentials =
      for (username <- conf.getOption(CassandraUserNameProperty);
           password <- conf.getOption(CassandraPasswordProperty)) yield (username, password)

    credentials match {
      case Some((user, password)) => PasswordAuthConfigurator(user, password)
      case None => NoAuthConfigurator
    }
  }
}

/** Entry point for obtaining `ConnectionConfigurator` object from `SparkConf`, used when establishing connections to Cassandra.
  * The actual `ConnectionConfigurator` creation is delegated to the [[ConnectionConfiguratorFactory]] pointed by
  * `spark.cassandra.auth.conf.factory.class` property. */
object ConnectionConfigurator {
  val ConnConfFactoryProperty = "spark.cassandra.connection.conf.factory.class"

  def fromSparkConf(conf: SparkConf) = {
    val connConfFactoryClass = conf.get(ConnConfFactoryProperty, classOf[DefaultConnConfFactory].getName)
    val connConfFactory = Class.forName(connConfFactoryClass).newInstance().asInstanceOf[ConnectionConfiguratorFactory]
    connConfFactory.configurator(conf)
  }
}
