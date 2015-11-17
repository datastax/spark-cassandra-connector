package com.datastax.spark.connector.cql

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider}
import com.datastax.spark.connector.util.{ConfigParameter, ReflectionUtil}
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
  val ReferenceSection = "Cassandra Authentication Parameters"

  val FactoryParam = ConfigParameter[AuthConfFactory](
    name = "spark.cassandra.auth.conf.factory",
    section = ReferenceSection,
    default = DefaultAuthConfFactory,
    description = "Name of a Scala module or class implementing AuthConfFactory providing custom authentication configuration"  )

  val Properties = Set(FactoryParam)

  def fromSparkConf(conf: SparkConf): AuthConfFactory = {
    conf
      .getOption(FactoryParam.name)
      .map(ReflectionUtil.findGlobalObject[AuthConfFactory])
      .getOrElse(FactoryParam.default)
  }
}

/** Default `AuthConfFactory` that supports no authentication or password authentication.
  * Password authentication is enabled when both `spark.cassandra.auth.username` and `spark.cassandra.auth.password`
  * options are present in [[org.apache.spark.SparkConf SparkConf]].*/
object DefaultAuthConfFactory extends AuthConfFactory {
  val referenceSection = "Default Authentication Parameters"

  val UserNameParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.auth.username",
    section = referenceSection,
    default = None,
    description = """Login name for password authentication""")

  val PasswordParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.auth.password",
    section = referenceSection,
    default = None,
    description = """password for password authentication""")

  override val properties = Set(
    UserNameParam.name,
    PasswordParam.name
  )

  def authConf(conf: SparkConf): AuthConf = {
    val credentials =
      for (username <- conf.getOption(UserNameParam.name);
           password <- conf.getOption(PasswordParam.name)) yield (username, password)

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



