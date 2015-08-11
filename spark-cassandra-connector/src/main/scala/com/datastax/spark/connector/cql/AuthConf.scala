package com.datastax.spark.connector.cql

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider}
import com.datastax.spark.connector.util.ReflectionUtil
import org.apache.spark.SparkConf

/** Stores credentials used to authenticate to a Cassandra cluster and uses them
  * to configure a Cassandra connection.
  * This driver provides implementations [[NoAuthConf]] for no authentication
  * and [[PasswordAuthConf]] for password authentication. Other authentication
  * configurators can be plugged in by setting `cassandra.authentication.conf.factory.class`
  * option. See [[AuthConfFactory]]. */
trait AuthConf extends Serializable {

  /** Returns auth provider to be passed to the `Cluster.Builder` object. */
  def authProvider: AuthProvider
}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
case object NoAuthConf extends AuthConf {
  override def authProvider = AuthProvider.NONE
}

/** Performs plain-text password authentication. Use with `PasswordAuthenticator` in Cassandra. */
case class PasswordAuthConf(user: String, password: String) extends AuthConf {
  override def authProvider = new PlainTextAuthProvider(user, password)
}

/** Obtains authentication configuration by reading  [[org.apache.spark.SparkConf SparkConf]] object. */
trait AuthConfFactory {

  def authConf(conf: SparkConf): AuthConf

  /** List of extra properties allowed in SparkConf passed to `authConf` method */
  def properties: Set[String] = Set.empty
}

object AuthConfFactory {
  val AuthConfFactoryProperty = "spark.cassandra.auth.conf.factory"
  val Properties = Set(AuthConfFactoryProperty)

  def fromSparkConf(conf: SparkConf): AuthConfFactory = {
    conf
      .getOption(AuthConfFactoryProperty)
      .map(ReflectionUtil.findGlobalObject[AuthConfFactory])
      .getOrElse(DefaultAuthConfFactory)
  }
}

/** Default `AuthConfFactory` that supports no authentication or password authentication.
  * Password authentication is enabled when both `spark.cassandra.auth.username` and `spark.cassandra.auth.password`
  * options are present in [[org.apache.spark.SparkConf SparkConf]].*/
object DefaultAuthConfFactory extends AuthConfFactory {

  val CassandraUserNameProperty = "spark.cassandra.auth.username"
  val CassandraPasswordProperty = "spark.cassandra.auth.password"

  override val properties = Set(
    CassandraUserNameProperty,
    CassandraPasswordProperty
  )

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

/** Entry point for obtaining `AuthConf` object from [[org.apache.spark.SparkConf SparkConf]], used when establishing connections to Cassandra.
  * The actual `AuthConf` creation is delegated to the [[AuthConfFactory]] pointed by `spark.cassandra.auth.conf.factory` property. */
object AuthConf {

  def fromSparkConf(conf: SparkConf) = {
    AuthConfFactory.fromSparkConf(conf).authConf(conf)
  }
}



