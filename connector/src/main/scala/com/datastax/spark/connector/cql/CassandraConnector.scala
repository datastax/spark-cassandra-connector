package com.datastax.spark.connector.cql

import java.io.IOException
import java.net.InetAddress

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.session.DefaultSession
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.util.ConfigCheck.ConnectorConfigurationException
import com.datastax.spark.connector.util.{Logging, SerialShutdownHooks}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

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
  * than `spark.cassandra.connection.keepAliveMS` property value.
  *
  * A `CassandraConnector` object is configured from [[CassandraConnectorConf]] object which
  * can be either given explicitly or automatically configured from [[org.apache.spark.SparkConf SparkConf]].
  * The connection options are:
  *   - `spark.cassandra.connection.host`:               contact points to connect to the Cassandra cluster, defaults to spark master host
  *   - `spark.cassandra.connection.port`:               Cassandra native port, defaults to 9042
  *   - `spark.cassandra.connection.factory`:            name of a Scala module or class implementing [[CassandraConnectionFactory]] that allows to plugin custom code for connecting to Cassandra
  *   - `spark.cassandra.connection.keepAliveMS`:      how long to keep unused connection before closing it (default 250 ms)
  *   - `spark.cassandra.connection.timeoutMS`:         how long to wait for connection to the Cassandra cluster (default 5 s)
  *   - `spark.cassandra.connection.reconnectionDelayMS.min`: initial delay determining how often to try to reconnect to a dead node (default 1 s)
  *   - `spark.cassandra.connection.reconnectionDelayMS.max`: final delay determining how often to try to reconnect to a dead node (default 60 s)
  *   - `spark.cassandra.auth.username`:                        login for password authentication
  *   - `spark.cassandra.auth.password`:                        password for password authentication
  *   - `spark.cassandra.auth.conf.factory`:                    name of a Scala module or class implementing [[AuthConfFactory]] that allows to plugin custom authentication configuration
  *   - `spark.cassandra.query.retry.count`:                    how many times to reattempt a failed query (default 10)
  *   - `spark.cassandra.read.timeoutMS`:                      maximum period of time to wait for a read to return
  *   - `spark.cassandra.connection.ssl.enabled`:               enable secure connection to Cassandra cluster
  *   - `spark.cassandra.connection.ssl.trustStore.path`:      path for the trust store being used
  *   - `spark.cassandra.connection.ssl.trustStore.password`:  trust store password
  *   - `spark.cassandra.connection.ssl.trustStore.type`:      trust store type (default JKS)
  *   - `spark.cassandra.connection.ssl.protocol`:              SSL protocol (default TLS)
  *   - `spark.cassandra.connection.ssl.enabledAlgorithms`:         SSL cipher suites (default TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA)
  */
class CassandraConnector(val conf: CassandraConnectorConf)
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
      _config = _config.copy(hosts = dataCenterNodes(_config, session))

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
  def withSessionDo[T](code: CqlSession => T): T = {
    closeResourceAfterUse(openSession()) { session =>
      code(SessionProxy.wrap(session))
    }
  }

  /**
    * Allows use of a Cassandra `Session` in a safe way without
    * risk of forgetting to close it from java. See `withSessionDo`
    *
    * We should not need this in scala 2.12
    */
  def jWithSessionDo[T](code: java.util.function.Function[CqlSession, T]): T = {
    withSessionDo(session => code.apply(session))
  }

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
    * Similar to try-with-resources in Java 7. */
  def closeResourceAfterUse[T, C <: { def close() }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

}

object CassandraConnector extends Logging {

  private[cql] val sessionCache = new RefCountedCache[CassandraConnectorConf, CqlSession](
    createSession, destroySession, alternativeConnectionConfigs)

  private def createSession(conf: CassandraConnectorConf): CqlSession = {
    lazy val endpointsStr = conf.hosts.map(_.getHostAddress).mkString("{", ", ", "}") + ":" + conf.port
    logDebug(s"Attempting to open native connection to Cassandra at $endpointsStr")
    try {
      val session = conf.connectionFactory.createSession(conf)
      logInfo(s"Connected to Cassandra cluster.")
      session
    }
    catch {
      case e: Throwable =>
        throw new IOException(s"Failed to open native connection to Cassandra at $endpointsStr", e)
    }
  }

  private def destroySession(session: CqlSession) {
    session.close()
    logInfo(s"Disconnected from Cassandra cluster.")
  }

  private def dataCenterNodes(conf: CassandraConnectorConf, session: CqlSession): Set[InetAddress] = {
    val allNodes = session.getMetadata.getNodes.asScala.values.toSet
    val dcToUse = conf.localDC.getOrElse(LocalNodeFirstLoadBalancingPolicy.determineDataCenter(conf.hosts, allNodes))
    val nodes = allNodes
      .collect { case v if v.getDatacenter == dcToUse && v.getBroadcastAddress.isPresent => v.getBroadcastAddress.get().getAddress }
    if (nodes.isEmpty) {
      throw new ConnectorConfigurationException(s"Could not determine suitable nodes for DC: $dcToUse and known nodes: " +
        s"${allNodes.map(n => (n.getHostId, n.getBroadcastAddress)).mkString(", ")}")
    }
    nodes
  }

  // This is to ensure the Cluster can be found by requesting for any of its hosts, or all hosts together.
  private def alternativeConnectionConfigs(conf: CassandraConnectorConf, session: CqlSession): Set[CassandraConnectorConf] = {
    val nodes = dataCenterNodes(conf, session)
    nodes.map(n => conf.copy(hosts = Set(n))) + conf.copy(hosts = nodes)
  }

  SerialShutdownHooks.add("Clearing session cache for C* connector", 200)(() => {
    sessionCache.shutdown()
  })

  /** Returns a CassandraConnector created from properties found in the [[org.apache.spark.SparkConf SparkConf]] object */
  def apply(conf: SparkConf): CassandraConnector = {
    new CassandraConnector(CassandraConnectorConf(conf))
  }

  /** Returns a CassandraConnector with runtime Cluster Environment information. This can set remoteConnectionsPerExecutor
    * based on cores and executors in use
    */
  def apply(sc: SparkContext): CassandraConnector = {
    val conf = CassandraConnectorConf(sc.getConf)
    val numCassandraCoresPerNode = 64 // guessed typical number of CPUs in each C* node - the goal is not to be too low
    val numExecutors: Int = math.max(Option(sc.getExecutorMemoryStatus).getOrElse(Map.empty).size, 1)
    val remoteConnections = Math.max(1, Math.round(Math.ceil(numCassandraCoresPerNode / numExecutors).toInt))
    val runtimeConf = conf.copy(
      remoteConnectionsPerExecutor = conf.remoteConnectionsPerExecutor orElse Some(remoteConnections))
    new CassandraConnector(runtimeConf)
  }

  /** Returns a CassandraConnector created from explicitly given connection configuration. */
  def apply(hosts: Set[InetAddress],
            port: Int = CassandraConnectorConf.ConnectionPortParam.default,
            authConf: AuthConf = NoAuthConf,
            localDC: Option[String] = None,
            keepAliveMillis: Int = CassandraConnectorConf.KeepAliveMillisParam.default,
            minReconnectionDelayMillis: Int = CassandraConnectorConf.MinReconnectionDelayParam.default,
            maxReconnectionDelayMillis: Int = CassandraConnectorConf.MaxReconnectionDelayParam.default,
            queryRetryCount: Int = CassandraConnectorConf.QueryRetryParam.default,
            connectTimeoutMillis: Int = CassandraConnectorConf.ConnectionTimeoutParam.default,
            readTimeoutMillis: Int = CassandraConnectorConf.ReadTimeoutParam.default,
            connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
            cassandraSSLConf: CassandraSSLConf = CassandraConnectorConf.DefaultCassandraSSLConf) = {

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
      cassandraSSLConf = cassandraSSLConf
    )

    new CassandraConnector(config)
  }

  def evictCache() {
    sessionCache.evict()
  }

}
