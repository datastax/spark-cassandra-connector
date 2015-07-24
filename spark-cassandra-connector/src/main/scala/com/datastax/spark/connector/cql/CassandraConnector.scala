package com.datastax.spark.connector.cql

import java.io.IOException
import java.net.InetAddress

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls

import org.apache.spark.{Logging, SparkConf}

import com.datastax.driver.core.{Cluster, Host, Session}
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf


/** Provides and manages connections to Cassandra.
  *
  * A `CassandraConnector` instance is serializable and
  * can be safely sent over network,
  * because it automatically reestablishes the connection
  * to the same cluster after deserialization. Internally it saves
  * a list of all nodes in the cluster, so a connection can be established
  * even if the host given in the initial config is down.
  *
  * Multiple `CassandraConnector`s in the same JVM connected to the same
  * Cassandra cluster will share a single underlying `Cluster` object.
  * `CassandraConnector` will close the underlying `Cluster` object automatically
  * whenever it is not used i.e. no `Session` or `Cluster` is open for longer
  * than `spark.cassandra.connection.keep_alive_ms` property value.
  *
  * A `CassandraConnector` object is configured from [[CassandraConnectorConf]] object which
  * can be either given explicitly or automatically configured from [[org.apache.spark.SparkConf SparkConf]].
  * The connection options are:
  *   - `spark.cassandra.connection.host`:               contact point to connect to the Cassandra cluster, defaults to spark master host
  *   - `spark.cassandra.connection.port`:               Cassandra native port, defaults to 9042
  *   - `spark.cassandra.connection.factory`:            name of a Scala module or class implementing [[CassandraConnectionFactory]] that allows to plugin custom code for connecting to Cassandra
  *   - `spark.cassandra.connection.keep_alive_ms`:      how long to keep unused connection before closing it (default 250 ms)
  *   - `spark.cassandra.connection.timeout_ms`:         how long to wait for connection to the Cassandra cluster (default 5 s)
  *   - `spark.cassandra.connection.reconnection_delay_ms.min`: initial delay determining how often to try to reconnect to a dead node (default 1 s)
  *   - `spark.cassandra.connection.reconnection_delay_ms.max`: final delay determining how often to try to reconnect to a dead node (default 60 s)
  *   - `spark.cassandra.auth.username`:                        login for password authentication
  *   - `spark.cassandra.auth.password`:                        password for password authentication
  *   - `spark.cassandra.auth.conf.factory`:                    name of a Scala module or class implementing [[AuthConfFactory]] that allows to plugin custom authentication configuration
  *   - `spark.cassandra.query.retry.count`:                    how many times to reattempt a failed query (default 10)
  *   - `spark.cassandra.query.retry.delay`:                    the delay between subsequent retries
  *   - `spark.cassandra.connection.ssl.enabled`:               enable secure connection to Cassandra cluster
  *   - `spark.cassandra.connection.ssl.trustStore.path`:      path for the trust store being used
  *   - `spark.cassandra.connection.ssl.trustStore.password`:  trust store password
  *   - `spark.cassandra.connection.ssl.trustStore.type`:      trust store type (default JKS)
  *   - `spark.cassandra.connection.ssl.protocol`:              SSL protocol (default TLS)
  *   - `spark.cassandra.connection.ssl.enabledAlgorithms`:         SSL cipher suites (default TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA)
  */
class CassandraConnector(conf: CassandraConnectorConf)
  extends Serializable with Logging {

  import com.datastax.spark.connector.cql.CassandraConnector._

  private[this] var _config = conf

  /** Known cluster hosts in the connected datacenter.*/
  lazy val hosts: Set[InetAddress] =
    // wrapped in a session, so we get full lists of hosts,
    // not only those explicitly passed in the conf
    withSessionDo { _ => _config.hosts }

  /** Configured native port */
  def port = _config.port
  
  /** Configured authentication options */
  def authConf = _config.authConf

  /** Connection configurator */
  def connectionFactory = _config.connectionFactory

  /** Returns a shared session to Cassandra and increases the internal open
    * reference counter. It does not release the session automatically,
    * so please remember to close it after use. Closing a shared session
    * decreases the session reference counter. If the reference count drops to zero,
    * the session may be physically closed. */
  def openSession() = {
    val session = sessionCache.acquire(_config)
    try {
      val allNodes = session.getCluster.getMetadata.getAllHosts.toSet
      val myNodes = LocalNodeFirstLoadBalancingPolicy
        .nodesInTheSameDC(_config.hosts, allNodes)
        .map(_.getAddress)
      _config = _config.copy(hosts = myNodes)

      // We need a separate SessionProxy here to protect against double closing the session.
      // Closing SessionProxy is not really closing the session, because sessions are shared.
      // Instead, refcount is decreased. But double closing the same Session reference must not
      // decrease refcount twice. There is a guard in SessionProxy
      // so any subsequent close calls on the same SessionProxy are no-ops.
      SessionProxy.wrapWithCloseAction(session)(sessionCache.release(_, _config.keepAliveMillis))
    }
    catch {
      case e: Throwable =>
        sessionCache.release(session, 0)
        throw e
    }
  }

  /** Allows to use Cassandra `Session` in a safe way without
    * risk of forgetting to close it. The `Session` object obtained through this method
    * is a proxy to a shared, single `Session` associated with the cluster.
    * Internally, the shared underlying `Session` will be closed shortly after all the proxies
    * are closed. */
  def withSessionDo[T](code: Session => T): T = {
    closeResourceAfterUse(openSession()) { session =>
      code(SessionProxy.wrap(session))
    }
  }

  /** Allows to use Cassandra `Cluster` in a safe way without
    * risk of forgetting to close it. Multiple, concurrent calls might share the same
    * `Cluster`. The `Cluster` will be closed when not in use for some time.
    * It is not recommended to obtain sessions from this method. Use [[withSessionDo]]
    * instead which allows for proper session sharing. */
  def withClusterDo[T](code: Cluster => T): T = {
    withSessionDo { session =>
      code(session.getCluster)
    }
  }

  /** Returns the local node, if it is one of the cluster nodes. Otherwise returns any node. */
  def closestLiveHost: Host = {
    withClusterDo { cluster =>
      LocalNodeFirstLoadBalancingPolicy
        .sortNodesByStatusAndProximity(_config.hosts, cluster.getMetadata.getAllHosts.toSet)
        .filter(_.isUp)
        .headOption
        .getOrElse(throw new IOException("Cannot connect to Cassandra: No live hosts found"))
    }
  }

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
    * Similar to try-with-resources in Java 7. */
  def closeResourceAfterUse[T, C <: { def close() }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

}

object CassandraConnector extends Logging {

  val keepAliveMillis = System.getProperty("spark.cassandra.connection.keep_alive_ms", "250").toInt

  private val sessionCache = new RefCountedCache[CassandraConnectorConf, Session](
    createSession, destroySession, alternativeConnectionConfigs)

  private def createSession(conf: CassandraConnectorConf): Session = {
    lazy val endpointsStr = conf.hosts.map(_.getHostAddress).mkString("{", ", ", "}") + ":" + conf.port
    logDebug(s"Attempting to open native connection to Cassandra at $endpointsStr")
    val cluster = conf.connectionFactory.createCluster(conf)
    try {
      val clusterName = cluster.getMetadata.getClusterName
      logInfo(s"Connected to Cassandra cluster: $clusterName")
      cluster.connect()
    }
    catch {
      case e: Throwable =>
        cluster.close()
        throw new IOException(s"Failed to open native connection to Cassandra at $endpointsStr", e)
    }
  }

  private def destroySession(session: Session) {
    val cluster = session.getCluster
    val clusterName = cluster.getMetadata.getClusterName
    session.close()
    cluster.close()
    PreparedStatementCache.remove(cluster)
    logInfo(s"Disconnected from Cassandra cluster: $clusterName")
  }

  // This is to ensure the Cluster can be found by requesting for any of its hosts, or all hosts together.
  private def alternativeConnectionConfigs(conf: CassandraConnectorConf, session: Session): Set[CassandraConnectorConf] = {
    val cluster = session.getCluster
    val hosts = LocalNodeFirstLoadBalancingPolicy.nodesInTheSameDC(conf.hosts, cluster.getMetadata.getAllHosts.toSet)
    hosts.map(h => conf.copy(hosts = Set(h.getAddress))) + conf.copy(hosts = hosts.map(_.getAddress))
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      sessionCache.shutdown()
    }
  }))

  /** Returns a CassandraConnector created from properties found in the [[org.apache.spark.SparkConf SparkConf]] object */
  def apply(conf: SparkConf): CassandraConnector = {
    new CassandraConnector(CassandraConnectorConf(conf))
  }

  /** Returns a CassandraConnector created from explicitly given connection configuration. */
  def apply(hosts: Set[InetAddress],
            port: Int = CassandraConnectorConf.DefaultPort,
            authConf: AuthConf = NoAuthConf,
            localDC: Option[String] = None,
            keepAliveMillis: Int = CassandraConnectorConf.DefaultKeepAliveMillis,
            minReconnectionDelayMillis: Int = CassandraConnectorConf.DefaultMinReconnectionDelayMillis,
            maxReconnectionDelayMillis: Int = CassandraConnectorConf.DefaultMaxReconnectionDelayMillis,
            queryRetryCount: Int = CassandraConnectorConf.DefaultQueryRetryCount,
            connectTimeoutMillis: Int = CassandraConnectorConf.DefaultConnectTimeoutMillis,
            readTimeoutMillis: Int = CassandraConnectorConf.DefaultReadTimeoutMillis,
            connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
            cassandraSSLConf: CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf,
            queryRetryDelay: CassandraConnectorConf.RetryDelayConf = CassandraConnectorConf.DefaultQueryRetryDelay) = {

    val config = CassandraConnectorConf(
      hosts = hosts,
      port = port,
      authConf = authConf,
      localDC = localDC,
      keepAliveMillis = keepAliveMillis,
      minReconnectionDelayMillis = minReconnectionDelayMillis,
      maxReconnectionDelayMillis = maxReconnectionDelayMillis,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeoutMillis,
      readTimeoutMillis = readTimeoutMillis,
      connectionFactory = connectionFactory,
      cassandraSSLConf = cassandraSSLConf,
      queryRetryDelay = queryRetryDelay
    )

    new CassandraConnector(config)
  }

  def evictCache() {
    sessionCache.evict()
  }

}
