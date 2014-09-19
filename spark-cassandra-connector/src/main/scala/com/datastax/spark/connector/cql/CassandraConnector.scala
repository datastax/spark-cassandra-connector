package com.datastax.spark.connector.cql

import java.io.IOException
import java.net.InetAddress

import com.datastax.driver.core.{Session, Host, Cluster}
import com.datastax.driver.core.policies._
import com.datastax.spark.connector.util.{Logging, IOUtils}

import org.apache.cassandra.thrift.Cassandra
import org.apache.spark.SparkConf
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.JavaConversions._


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
  * can be either given explicitly or automatically configured from `SparkConf`.
  * The connection options are:
  *   - `spark.cassandra.connection.host`:         contact point to connect to the Cassandra cluster, defaults to spark master host
  *   - `spark.cassandra.connection.rpc.port`:     Cassandra thrift port, defaults to 9160
  *   - `spark.cassandra.connection.native.port`:  Cassandra native port, defaults to 9042
  *   - `spark.cassandra.auth.username`:           login for password authentication
  *   - `spark.cassandra.auth.password`:           password for password authentication
  *   - `spark.cassandra.auth.conf.factory.class`: name of the class implementing [[AuthConfFactory]] that allows to plugin custom authentication
  *
  * Additionally this object uses the following global System properties:
  *   - `spark.cassandra.connection.keep_alive_ms`: the number of milliseconds to keep unused `Cluster` object before destroying it (default 100 ms)
  *   - `spark.cassandra.connection.reconnection_delay_ms.min`: initial delay determining how often to try to reconnect to a dead node (default 1 s)
  *   - `spark.cassandra.connection.reconnection_delay_ms.max`: final delay determining how often to try to reconnect to a dead node (default 60 s)
  *   - `spark.cassandra.query.retry.count`: how many times to reattempt a failed query 
  */
class CassandraConnector(conf: CassandraConnectorConf)
  extends Serializable with Logging {

  import com.datastax.spark.connector.cql.CassandraConnector._

  private[this] var _config = conf

  /** Known cluster hosts. This is going to return all cluster hosts after at least one successful connection has been made */
  def hosts = _config.hosts

  /** Configured native port */
  def nativePort = _config.nativePort

  /** Configured thrift client port */
  def rpcPort = _config.rpcPort

  /** Authentication configuration */
  def authConf = _config.authConf

  /** Returns a shared session to Cassandra and increases the internal open
    * reference counter. It does not release the session automatically,
    * so please remember to close it after use. Closing a shared session
    * decreases the session reference counter. If the reference count drops to zero,
    * the session may be physically closed. */
  def openSession() = {
    val session = sessionCache.acquire(_config)
    try {
      val allNodes = session.getCluster.getMetadata.getAllHosts.toSet
      val myNodes = LocalNodeFirstLoadBalancingPolicy.nodesInTheSameDC(_config.hosts, allNodes).map(_.getAddress)
      _config = _config.copy(hosts = myNodes)

      // We need a separate SessionProxy here to protect against double closing the session.
      // Closing SessionProxy is not really closing the session, because sessions are shared.
      // Instead, refcount is decreased. But double closing the same Session reference must not
      // decrease refcount twice. There is a guard in SessionProxy
      // so any subsequent close calls on the same SessionProxy are a no-ops.
      SessionProxy.wrapWithCloseAction(session)(sessionCache.release)
    } catch {
      case e: Throwable =>
        sessionCache.release(session)
        throw e
    }
  }

  /** Allows to use Cassandra `Session` in a safe way without
    * risk of forgetting to close it. The `Session` object obtained through this method
    * is a proxy to a shared, single `Session` associated with the cluster.
    * Internally, the shared underlying `Session` will be closed shortly after all the proxies
    * are closed. */
  def withSessionDo[T](code: Session => T): T = {
    IOUtils.closeAfterUse(openSession()) { session =>
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
        .sortNodesByProximityAndStatus(_config.hosts, cluster.getMetadata.getAllHosts.toSet)
        .headOption
        .getOrElse(throw new IOException("Cannot connect to Cassandra: No hosts found"))
    }
  }

  /** Opens a Thrift client to the given host. Don't use it unless you really know what you are doing. */
  def createThriftClient(host: InetAddress): CassandraClientProxy = {
    val transportFactory = conf.authConf.transportFactory
    val transport = transportFactory.openTransport(host.getHostAddress, rpcPort)
    val client = new Cassandra.Client(new TBinaryProtocol.Factory().getProtocol(transport))
    conf.authConf.configureThriftClient(client)
    CassandraClientProxy.wrap(client, transport)
  }

  def createThriftClient(): CassandraClientProxy =
    createThriftClient(closestLiveHost.getAddress)

  def withCassandraClientDo[T](host: InetAddress)(code: CassandraClientProxy => T): T =
    IOUtils.closeAfterUse(createThriftClient(host))(code)

  def withCassandraClientDo[T](code: CassandraClientProxy => T): T =
    IOUtils.closeAfterUse(createThriftClient())(code)

}

object CassandraConnector extends Logging {
  val keepAliveMillis = System.getProperty("spark.cassandra.connection.keep_alive_ms", "250").toInt
  val minReconnectionDelay = System.getProperty("spark.cassandra.connection.reconnection_delay_ms.min", "1000").toInt
  val maxReconnectionDelay = System.getProperty("spark.cassandra.connection.reconnection_delay_ms.max", "60000").toInt
  val retryCount = System.getProperty("spark.cassandra.query.retry.count", "10").toInt

  private val sessionCache = new RefCountedCache[CassandraConnectorConf, Session](
    createSession, destroySession, alternativeConnectionConfigs, releaseDelayMillis = keepAliveMillis)

  private def createSession(conf: CassandraConnectorConf): Session = {
    logDebug(s"Connecting to cluster: ${conf.hosts.mkString("{", ",", "}")}:${conf.nativePort}")
    val cluster =
      Cluster.builder()
        .addContactPoints(conf.hosts.toSeq: _*)
        .withPort(conf.nativePort)
        .withRetryPolicy(new MultipleRetryPolicy(retryCount))
        .withReconnectionPolicy(new ExponentialReconnectionPolicy(minReconnectionDelay, maxReconnectionDelay))
        .withLoadBalancingPolicy(new LocalNodeFirstLoadBalancingPolicy(conf.hosts))
        .withAuthProvider(conf.authConf.authProvider)
        .build()

    try {
      val clusterName = cluster.getMetadata.getClusterName
      logInfo(s"Connected to Cassandra cluster: $clusterName")
      cluster.connect()
    }
    catch {
      case e: Throwable =>
        cluster.close()
        throw e
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

  /** Returns a CassandraConnector created from properties found in the `SparkConf` object */
  def apply(conf: SparkConf): CassandraConnector = {
    new CassandraConnector(CassandraConnectorConf.apply(conf))
  }

  /** Returns a CassandraConnector created from explicitly given connection configuration. */
  def apply(host: InetAddress,
            nativePort: Int = CassandraConnectorConf.DefaultNativePort,
            rpcPort: Int = CassandraConnectorConf.DefaultRpcPort,
            authConf: AuthConf = NoAuthConf) = {

    val config = CassandraConnectorConf.apply(host, nativePort, rpcPort, authConf)
    new CassandraConnector(config)
  }

  def evictCache() {
    sessionCache.evict()
  }

}
