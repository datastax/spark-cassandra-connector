package com.datastax.spark.connector.cql

import java.net.{InetAddress, NetworkInterface}
import java.nio.ByteBuffer
import java.util.{Collection => JCollection, Iterator => JIterator}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.spark.connector.util.Logging

import scala.collection.JavaConversions._
import scala.util.Random

/** Selects local node first and then nodes in local DC in random order. Never selects nodes from other DCs.
  * For writes, if a statement has a routing key set, this LBP is token aware - it prefers the nodes which
  * are replicas of the computed token to the other nodes. */
class LocalNodeFirstLoadBalancingPolicy(contactPoints: Set[InetAddress], localDC: Option[String] = None,
                                        shuffleReplicas: Boolean = true) extends LoadBalancingPolicy with Logging {

  import LocalNodeFirstLoadBalancingPolicy._

  private var nodes = Set.empty[Host]
  private var dcToUse = ""
  private val random = new Random
  private var clusterMetadata: Metadata = _

  override def distance(host: Host): HostDistance =
    if (host.getDatacenter == dcToUse) {
      sameDCHostDistance(host)
    } else {
      // this insures we keep remote hosts out of our list entirely, even when we get notified of newly joined nodes
      HostDistance.IGNORED
    }

  override def init(cluster: Cluster, hosts: JCollection[Host]) {
    nodes = hosts.toSet
    // use explicitly set DC if available, otherwise see if all contact points have same DC
    // if so, use that DC; if not, throw an error
    dcToUse = localDC.getOrElse(determineDataCenter(contactPoints, nodes))
    clusterMetadata = cluster.getMetadata
  }

  private def tokenUnawareQueryPlan(query: String, statement: Statement): JIterator[Host] = {
    sortNodesByStatusAndProximity(dcToUse, nodes).iterator
  }

  private def findReplicas(keyspace: String, partitionKey: ByteBuffer): Set[Host] = {
    clusterMetadata.getReplicas(Metadata.quote(keyspace), partitionKey).toSet
      .filter(host => host.isUp && distance(host) != HostDistance.IGNORED)
  }

  private def tokenAwareQueryPlan(keyspace: String, statement: Statement): JIterator[Host] = {
    assert(keyspace != null)
    assert(statement.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE) != null)

    val replicas = findReplicas(keyspace,
      statement.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE))
    val (localReplica, otherReplicas) = replicas.partition(isLocalHost)
    lazy val maybeShuffled = if (shuffleReplicas) random.shuffle(otherReplicas.toIndexedSeq) else otherReplicas

    lazy val otherHosts = tokenUnawareQueryPlan(keyspace, statement).toIterator
      .filter(host => !replicas.contains(host) && distance(host) != HostDistance.IGNORED)

    (localReplica.iterator #:: maybeShuffled.iterator #:: otherHosts #:: Stream.empty).flatten.iterator
  }

  override def newQueryPlan(loggedKeyspace: String, statement: Statement): JIterator[Host] = {
    val keyspace = if (statement.getKeyspace == null) loggedKeyspace else statement.getKeyspace

    if (statement.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE) == null || keyspace == null)
      tokenUnawareQueryPlan(keyspace, statement)
    else
      tokenAwareQueryPlan(keyspace, statement)
  }
  
  override def onAdd(host: Host) {
    // The added host might be a "better" version of a host already in the set.
    // The nodes added in the init call don't have DC and rack set.
    // Therefore we want to really replace the object now, to get full information on DC:
    nodes -= host
    nodes += host
    logInfo(s"Added host ${host.getAddress.getHostAddress} (${host.getDatacenter})")
  }
  override def onRemove(host: Host) {
    nodes -= host
    logInfo(s"Removed host ${host.getAddress.getHostAddress} (${host.getDatacenter})")
  }

  override def close() = { }
  override def onUp(host: Host) = { }
  override def onDown(host: Host) = { }

  private def sameDCHostDistance(host: Host) =
    if (isLocalHost(host))
      HostDistance.LOCAL
    else
      HostDistance.REMOTE

  private def dcs(hosts: Set[Host]) =
    hosts.filter(_.getDatacenter != null).map(_.getDatacenter).toSet
}

object LocalNodeFirstLoadBalancingPolicy {

  private val random = new Random

  private val localAddresses =
    NetworkInterface.getNetworkInterfaces.flatMap(_.getInetAddresses).toSet

  /** Returns true if given host is local host */
  def isLocalHost(host: Host): Boolean = {
    val hostAddress = host.getAddress
    hostAddress.isLoopbackAddress || localAddresses.contains(hostAddress)
  }

  /** Sorts nodes in the following order:
    * 1. local node in a given DC
    * 2. live nodes in a given DC
    * 3. the rest of nodes in a given DC
    *
    * Nodes within a group are ordered randomly. Nodes from other DCs are not included. */
  def sortNodesByStatusAndProximity(dc: String, hostsToSort: Set[Host]): Seq[Host] = {
    val grouped = hostsToSort.groupBy {
      case host if host.getDatacenter != dc => None
      case host if !host.isUp => Some(2)
      case host if !isLocalHost(host) => Some(1)
      case _ => Some(0)
    } - None

    grouped.toSeq.sortBy(_._1.get).flatMap {
      case (_, hosts) => random.shuffle(hosts.toIndexedSeq)
    }
  }

  /** Returns a common data center name of the given contact points.
    *
    * For each contact point there must be a [[Host]] in `allHosts` collection in order to determine its data center
    * name. If contact points belong to more than a single data center, an [[IllegalArgumentException]] is thrown.
    */
  def determineDataCenter(contactPoints: Set[InetAddress], allHosts: Set[Host]): String = {
    val dcs = allHosts
      .filter(host => contactPoints.contains(host.getAddress))
      .flatMap(host => Option(host.getDatacenter))
    assert(dcs.nonEmpty, "There are no contact points in the given set of hosts")
    require(dcs.size == 1, s"Contact points contain multiple data centers: ${dcs.mkString(", ")}")
    dcs.head
  }

}
