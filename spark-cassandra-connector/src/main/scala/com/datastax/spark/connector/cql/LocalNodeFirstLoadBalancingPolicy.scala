package com.datastax.spark.connector.cql

import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.{Statement, Cluster, HostDistance, Host}
import java.net.{InetAddress, NetworkInterface}
import scala.collection.JavaConversions._
import scala.util.Random

import org.apache.spark.Logging

/** Selects local node first and then nodes in local DC in random order. Never selects nodes from other DCs. */
class LocalNodeFirstLoadBalancingPolicy(contactPoints: Set[InetAddress], localDC: Option[String] = None) extends LoadBalancingPolicy with Logging {

  import LocalNodeFirstLoadBalancingPolicy._

  private var liveNodes = Set.empty[Host]
  private var dcToUse = ""
  private val random = new Random

  override def distance(host: Host): HostDistance =
    if (host.getDatacenter == dcToUse) {
      logInfo(s"Adding host ${host.getAddress.getHostAddress} (${host.getDatacenter})")
      sameDCHostDistance(host)
    } else {
      // this insures we keep remote hosts out of our list entirely, even when we get notified of newly joined nodes
      logInfo(s"Ignoring remote host ${host.getAddress.getHostAddress} (${host.getDatacenter})")
      HostDistance.IGNORED
    }

  override def init(cluster: Cluster, hosts: java.util.Collection[Host]) {
    liveNodes = hosts.filter(_.isUp).toSet
    // use explicitly set DC if available, otherwise see if all contact points have same DC
    // if so, use that DC; if not, throw an error
    dcToUse = localDC match { 
      case Some(local) => local
      case None => 
        val dcList = dcs(nodesInTheSameDC(contactPoints, hosts.toSet))
        if (dcList.size == 1) 
          dcList.head
        else 
          throw new IllegalArgumentException(s"Contact points contain multiple data centers: ${dcList.mkString(", ")}")
    }
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

  private def sameDCHostDistance(host: Host) =
    if (isLocalHost(host))
      HostDistance.LOCAL
    else
      HostDistance.REMOTE

  private def dcs(hosts: Set[Host]) = hosts.map(_.getDatacenter).toSet
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