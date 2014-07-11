package com.datastax.spark.connector.cql

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider, Cluster}
import com.datastax.driver.core.Cluster.Builder
import org.apache.cassandra.thrift.{TFramedTransportFactory, ITransportFactory, AuthenticationRequest, Cassandra}
import org.apache.cassandra.thrift.Cassandra.Iface
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/** Stores credentials used to authenticate to a Cassandra cluster and uses them
  * to configure a Cassandra connection.
  * This driver provides implementations [[NoAuthConf]] for no authentication
  * and [[PasswordAuthConf]] for password authentication. Other authentication
  * configurators can be plugged in by setting `cassandra.authentication.conf.factory.class`
  * option. See [[AuthConfFactory]]. */
trait AuthConf extends Serializable {

  /** Returns auth provider to be passed to the `Cluster.Builder` object. */
  def authProvider: AuthProvider

  /** Returns the transport factory for creating thrift transports.
    * Some authentication mechanisms like SASL require custom thrift transport.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def transportFactory: ITransportFactory

  /** Sets appropriate authentication options for Thrift connection.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def configureThriftClient(client: Cassandra.Iface)

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
case object NoAuthConf extends AuthConf {
  def authProvider = AuthProvider.NONE
  def transportFactory = new TFramedTransportFactory
  def configureThriftClient(client: Iface) {}

}

/** Performs plain-text password authentication. Use with `PasswordAuthenticator` in Cassandra. */
case class PasswordAuthConf(user: String, password: String) extends AuthConf {

  def authProvider = new PlainTextAuthProvider(user, password)
  def transportFactory = new TFramedTransportFactory

  def configureThriftClient(client: Iface) = {
    val authRequest = new AuthenticationRequest(Map("username" -> user, "password" -> password))
    client.login(authRequest)
  }
}

/** Obtains authentication configuration by reading  `SparkConf` object. */
trait AuthConfFactory {
  def authConf(conf: SparkConf): AuthConf
}

/** Default `AuthConfFactory` that supports no authentication or password authentication.
  * Password authentication is enabled when both `cassandra.username` and `cassandra.password`
  * options are present in `SparkConf`.*/
class DefaultAuthConfFactory extends AuthConfFactory {

  val CassandraUserNameProperty = "cassandra.username"
  val CassandraPasswordProperty = "cassandra.password"

  def authConf(conf: SparkConf): AuthConf = {
    val credentials = 
      for (username <- conf.getOption(CassandraUserNameProperty);
           password <- conf.getOption(CassandraPasswordProperty)) yield (username, password)

    credentials match {
      case Some((user, password)) => PasswordAuthConf(user, password)
      case None => NoAuthConf
    }
  }
}

/** Entry point for obtaining `AuthConf` object from `SparkConf`, used when establishing connections to Cassandra.
  * The actual `AuthConf` creation is delegated to the [[AuthConfFactory]] pointed by `cassandra.auth.conf.factory.class` property. */
object AuthConf {
  val AuthConfFactoryProperty = "cassandra.auth.conf.factory.class"

  def fromSparkConf(conf: SparkConf) = {
    val authConfFactoryClass = conf.get(AuthConfFactoryProperty, classOf[DefaultAuthConfFactory].getName)
    val authConfFactory = Class.forName(authConfFactoryClass).newInstance().asInstanceOf[AuthConfFactory]
    authConfFactory.authConf(conf)
  }
}



