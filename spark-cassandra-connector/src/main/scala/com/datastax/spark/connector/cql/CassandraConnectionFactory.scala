package com.datastax.spark.connector.cql

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf

import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{Cluster, SSLOptions, SocketOptions}
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.util.ReflectionUtil

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory` option. */
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster

  /** List of allowed custom property names passed in SparkConf */
  def properties: Set[String] = Set.empty
}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object DefaultConnectionFactory extends CassandraConnectionFactory {

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    val builder = Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      .withRetryPolicy(
        new MultipleRetryPolicy(conf.queryRetryCount, conf.queryRetryDelay))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
      .withLoadBalancingPolicy(
        new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
      .withCompression(conf.compression)

    if (conf.cassandraSSLConf.enabled) {
      maybeCreateSSLOptions(conf.cassandraSSLConf) match {
        case Some(sslOptions) ⇒ builder.withSSL(sslOptions)
        case None ⇒ builder.withSSL()
      }
    } else {
      builder
    }
  }

  private def maybeCreateSSLOptions(conf: CassandraSSLConf): Option[SSLOptions] = {
    conf.trustStorePath map {
      case path ⇒

        val trustStoreFile = new FileInputStream(path)
        val tmf = try {
          val keyStore = KeyStore.getInstance(conf.trustStoreType)
          conf.trustStorePassword match {
            case None ⇒ keyStore.load(trustStoreFile, null)
            case Some(password) ⇒ keyStore.load(trustStoreFile, password.toCharArray)
          }
          val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
          tmf.init(keyStore)
          tmf
        } finally {
          IOUtils.closeQuietly(trustStoreFile)
        }

        val context = SSLContext.getInstance(conf.protocol)
        context.init(null, tmf.getTrustManagers, new SecureRandom)
        new SSLOptions(context, conf.enabledAlgorithms)
    }
  }

  /** Creates and configures native Cassandra connection */
  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    clusterBuilder(conf).build()
  }

}

/** Entry point for obtaining `CassandraConnectionFactory` object from [[org.apache.spark.SparkConf SparkConf]],
  * used when establishing connections to Cassandra. */
object CassandraConnectionFactory {
  val ConnectionFactoryProperty = "spark.cassandra.connection.factory"
  val Properties = Set(ConnectionFactoryProperty)

  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    conf.getOption(ConnectionFactoryProperty)
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(DefaultConnectionFactory)
  }
}
