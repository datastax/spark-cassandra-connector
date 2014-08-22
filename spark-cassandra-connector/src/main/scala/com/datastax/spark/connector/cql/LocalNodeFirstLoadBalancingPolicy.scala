package com.datastax.spark.connector.cql

import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.{Statement, Cluster, HostDistance, Host}
import java.net.{InetAddress, NetworkInterface}
import scala.collection.JavaConversions._
import scala.util.Random

/** Selects local node first and then nodes in local DC in random order. Never selects nodes from other DCs. */
class LocalNodeFirstLoadBalancingPolicy(contactPoints: Set[InetAddress]) extends LoadBalancingPolicy {

  import LocalNodeFirstLoadBalancingPolicy._

  private var liveNodes = Set.empty[Host]
  private val random = new Random

  override def distance(host: Host): HostDistance =
    if (isLocalHost(host))
      HostDistance.LOCAL
    else
      HostDistance.REMOTE

  override def init(cluster: Cluster, hosts: java.util.Collection[Host]) {
    liveNodes = hosts.filter(_.isUp).toSet
  }

  override def newQueryPlan(query: String, statement: Statement): java.util.Iterator[Host] = {
    sortNodesByProximityAndStatus(contactPoints, liveNodes).iterator
  }

  override def onAdd(host: Host) {
    // The added host might be a "better" version of a host already in the set.
    // The nodes added in the init call don't have DC and rack set.
    // Therefore we want to really replace the object now, to get full information on DC:
    liveNodes -= host
    liveNodes += host
  }

  override def onRemove(host: Host) { liveNodes -= host }
  override def onUp(host: Host) = { }
  override def onDown(host: Host) = { }
  override def onSuspected(host: Host) = { liveNodes += host }
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

  /** Finds the DCs of the contact points and returns hosts in those DC(s) from `allHosts` */
  def nodesInTheSameDC(contactPoints: Set[InetAddress], allHosts: Set[Host]): Set[Host] = {
    val contactNodes = allHosts.filter(h => contactPoints.contains(h.getAddress))
    val contactDCs =  contactNodes.map(_.getDatacenter).filter(_ != null).toSet
    allHosts.filter(h => h.getDatacenter == null || contactDCs.contains(h.getDatacenter))
  }

  /** Sorts nodes in the following order:
    * 1. local host
    * 2. live nodes in the same DC as `contactPoints`
    * 3. down nodes in the same DC as `contactPoints`
    *
    * Nodes within a group are ordered randomly.
    * Nodes from other DCs are not included. */
  def sortNodesByProximityAndStatus(contactPoints: Set[InetAddress], hostsToSort: Set[Host]): Seq[Host] = {
    val nodesInLocalDC = nodesInTheSameDC(contactPoints, hostsToSort)
    val (localHost, otherHosts) = nodesInLocalDC.partition(isLocalHost)
    val (upHosts, downHosts) = otherHosts.partition(_.isUp)
    localHost.toSeq ++ random.shuffle(upHosts.toSeq) ++ random.shuffle(downHosts.toSeq)
  }

}