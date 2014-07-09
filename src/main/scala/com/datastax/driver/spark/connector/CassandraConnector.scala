package com.datastax.driver.spark.connector

import java.net.InetAddress

import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import com.datastax.driver.spark.util.IOUtils
import org.apache.cassandra.thrift.Cassandra
import org.apache.spark.{Logging, SparkConf}
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.JavaConversions._
import scala.util.Random


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
  * than `cassandra.connection.keep_alive_ms` property value.
  *
  * A `CassandraConnector` object is configured from [[CassandraConnectorConf]] object which
  * can be either given explicitly or automatically configured from `SparkConf`.
  * The connection options are:
  *   - `cassandra.connection.host`:         contact point to connect to the Cassandra cluster, defaults to spark master host
  *   - `cassandra.connection.rpc.port`:     Cassandra thrift port, defaults to 9160
  *   - `cassandra.connection.native.port`:  Cassandra native port, defaults to 9042
  *   - `cassandra.username`:                login for password authentication
  *   - `cassandra.password`:                password for password authentication
  *   - `cassandra.auth.conf.factory.class`: name of the class implementing [[AuthConfFactory]] that allows to plugin custom authentication
  *
  * Additionally this object uses the following global System properties:
  *   - `cassandra.connection.keep_alive_ms`: the number of milliseconds to keep unused `Cluster` object before destroying it (default 100 ms)
  *   - `cassandra.connection.reconnection_delay_ms.min`: initial delay determining how often to try to reconnect to a dead node (default 1 s)
  *   - `cassandra.connection.reconnection_delay_ms.max`: final delay determining how often to try to reconnect to a dead node (default 60 s)
  *   - `cassandra.query.retry.count`: how many times to reattempt a failed query 
  */
class CassandraConnector(conf: CassandraConnectorConf)
  extends Serializable with Logging {

  import com.datastax.driver.spark.connector.CassandraConnector._

  private[this] var _config = conf

  /** Known cluster hosts. This is going to return all cluster hosts after at least one successful connection has been made */
  def hosts = _config.hosts

  /** Configured native port */
  def nativePort = _config.nativePort

  /** Configured thrift client port */
  def rpcPort = _config.rpcPort

  /** Authentication configuration */
  def authConf = _config.authConf

  /** Allows to use Cassandra `Cluster` in a safe way without
    * risk of forgetting to close it. Multiple, concurrent calls might share the same
    * `Cluster`. The `Cluster` will be closed when not in use for some time. */
  def withClusterDo[T](code: Cluster => T): T = {
    var cluster: Cluster = null
    try {
      cluster = clusterCache.acquire(_config)
      val allNodes = cluster.getMetadata.getAllHosts.toSet
      val myNodes = nodesInTheSameDC(_config.hosts, allNodes).map(_.getAddress)
      _config = _config.copy(hosts = myNodes)
      code(cluster)
    }
    finally {
      if (cluster != null)
        clusterCache.release(cluster)
    }
  }

  /** Allows to use Cassandra `Session` in a safe way without
    * risk of forgetting to close it. */
  def withSessionDo[T](code: Session => T): T = {
    withClusterDo { cluster =>
      IOUtils.closeAfterUse(cluster.connect) { session =>
        code(SessionProxy.wrap(session))
      }
    }
  }

  /** Opens a new session to Cassandra.
    * It does not close it automatically, so please remember to close it after use. */
  def openSession() = {
    var cluster: Cluster = null
    var session: Session = null
    try {
      cluster = clusterCache.acquire(_config)
      session = cluster.connect()
      SessionProxy.wrapWithCloseAction(session) { _ =>
        clusterCache.release(cluster)
      }
    }
    catch {
      case e: Throwable =>
        if (session != null)
          session.close()
        if (cluster != null)
          clusterCache.release(cluster)
        throw e
    }
  }

  /** Returns the local node, if it is one of the cluster nodes. Otherwise returns any node. */
  def closestLiveHost: Host = {
    withClusterDo { cluster =>
      val liveHosts = cluster.getMetadata.getAllHosts.filter(_.isUp)
      val localHost = liveHosts.find(LocalNodeFirstLoadBalancingPolicy.isLocalHost)
      (localHost ++ Random.shuffle(liveHosts)).head
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
  val keepAliveMillis = System.getProperty("cassandra.connection.keep_alive_ms", "250").toInt
  val minReconnectionDelay = System.getProperty("cassandra.connection.reconnection_delay_ms.min", "1000").toInt
  val maxReconnectionDelay = System.getProperty("cassandra.connection.reconnection_delay_ms.max", "60000").toInt
  val retryCount = System.getProperty("cassandra.query.retry.count", "10").toInt

  private val clusterCache = new RefCountedCache[CassandraConnectorConf, Cluster](
    createCluster, destroyCluster, alternativeConnectionConfigs, releaseDelayMillis = keepAliveMillis)

  private def createCluster(conf: CassandraConnectorConf): Cluster = {
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
    val clusterName = cluster.getMetadata.getClusterName
    logInfo(s"Connected to Cassandra cluster: $clusterName")
    cluster
  }

  private def destroyCluster(cluster: Cluster) {
    val clusterName = cluster.getMetadata.getClusterName
    cluster.close()
    PreparedStatementCache.remove(cluster)
    logInfo(s"Disconnected from Cassandra cluster: $clusterName")
  }

  // This is to ensure the Cluster can be found by requesting for any of its hosts, or all hosts together.
  private def alternativeConnectionConfigs(conf: CassandraConnectorConf, cluster: Cluster): Set[CassandraConnectorConf] = {
    val hosts = nodesInTheSameDC(conf.hosts, cluster.getMetadata.getAllHosts.toSet)
    hosts.map(h => conf.copy(hosts = Set(h.getAddress))) + conf.copy(hosts = hosts.map(_.getAddress))
  }

  /** Finds the DCs of the contact points and returns hosts in those DC(s) from `allHosts` */
  def nodesInTheSameDC(contactPoints: Set[InetAddress], allHosts: Set[Host]): Set[Host] = {
    val contactNodes = allHosts.filter(h => contactPoints.contains(h.getAddress))
    val contactDCs =  contactNodes.map(_.getDatacenter).filter(_ != null).toSet
    allHosts.filter(h => h.getDatacenter == null || contactDCs.contains(h.getDatacenter))
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      clusterCache.shutdown()
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
            authConfig: AuthConf = NoAuthConf) = {

    val config = CassandraConnectorConf.apply(host, nativePort, rpcPort, authConfig)
    new CassandraConnector(config)
  }

}
