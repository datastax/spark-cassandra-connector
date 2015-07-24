package com.datastax.spark.connector.cql

import java.io.FileInputStream
import java.net.InetAddress
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import scala.collection.JavaConversions._

import org.apache.cassandra.thrift._
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TTransport

import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{Cluster, SSLOptions, SocketOptions}
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.util.ReflectionUtil

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory` option. */
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress): (Cassandra.Iface, TTransport)

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster

  /** List of allowed custom property names passed in SparkConf */
  def properties: Set[String] = Set.empty
}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object DefaultConnectionFactory extends CassandraConnectionFactory {

  /** Creates and configures a Thrift client.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  override def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress) = {
    var transport: TTransport = null
    try {
      val transportFactory = createTransportFactory(conf.cassandraSSLConf)
      val options = getTransportFactoryOptions(transportFactory.supportedOptions().toSet, conf)
      transportFactory.setOptions(options)

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

  def getTransportFactoryOptions(supportedOptions: Set[String], conf: CassandraConnectorConf) = Seq(
    conf.cassandraSSLConf.trustStorePath.map("enc.truststore" → _),
    conf.cassandraSSLConf.trustStorePassword.map("enc.truststore.password" → _),
    Some("enc.protocol" → conf.cassandraSSLConf.protocol),
    Some("enc.cipher.suites" → conf.cassandraSSLConf.enabledAlgorithms.mkString(","))
  ).flatten.toMap.filterKeys(supportedOptions.contains)

  def createTransportFactory(conf: CassandraSSLConf): ITransportFactory = {
    conf.enabled match {
      case false ⇒ new TFramedTransportFactory()
      case true ⇒
        val factoryClass = Class.forName("org.apache.cassandra.thrift.SSLTransportFactory")
        val factory = factoryClass.newInstance()
        factory.asInstanceOf[ITransportFactory]
    }
  }

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    val builder = Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.nativePort)
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
