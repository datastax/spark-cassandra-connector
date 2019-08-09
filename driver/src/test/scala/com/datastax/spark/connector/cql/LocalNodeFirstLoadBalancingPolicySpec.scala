/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.cql

import java.net.InetAddress

import org.scalatest.{FlatSpec, Matchers}


class LocalNodeFirstLoadBalancingPolicySpec extends FlatSpec with Matchers {

  private val localNode0 = InetAddress.getLoopbackAddress
  private val remoteNode1 = InetAddress.getByName("192.168.123.1")
  private val remoteNode2 = InetAddress.getByName("192.168.123.2")
  private val remoteNode3 = InetAddress.getByName("192.168.123.3")
  private val remoteNode4 = InetAddress.getByName("192.168.123.4")
  private val dc = "superDC"
  /* TODO: ReplicaAwareStatement was removed, `setRoutingToken` is used instead - rework following test:
  private def policyForClusterOf(nodes: InetAddress*) = {
    val policy = new LocalNodeFirstLoadBalancingPolicy(null, Some(dc))

    val hosts = nodes.map { address =>
      val host = mock(classOf[Host])
      when(host.getAddress).thenReturn(address)
      when(host.getDatacenter).thenReturn(dc)
      when(host.isUp).thenReturn(true)
      host
    }.toSet

    val metadata = mock(classOf[Metadata])
    when(metadata.getAllHosts).thenReturn(hosts)

    val cluster = mock(classOf[Cluster])
    when(cluster.getMetadata).thenReturn(metadata)

    policy.init(cluster, hosts)
    policy
  }

  private def replicateAwareStatementFor(replicas: InetAddress*) = {
    val stmt = mock(classOf[Statement])
    when(stmt.getKeyspace).thenReturn(dc)
    new ReplicaAwareStatement(stmt, replicas.toSet)
  }

  "LocalNodeFirstLoadBalancingPolicy" should "favor supplied replicas" in {
    val policy = policyForClusterOf(remoteNode1, remoteNode2, remoteNode3, remoteNode4)
    val statement = replicateAwareStatementFor(remoteNode3, remoteNode4)

    val plan = policy.newQueryPlan("superKeyspace", statement).toSeq.map(_.getAddress)

    plan.size should be(4)
    plan.take(2) should contain only(remoteNode3, remoteNode4)
    plan.takeRight(2) should contain only(remoteNode1, remoteNode2)
  }

  it should "favor local supplied replicas" in {
    val policy = policyForClusterOf(remoteNode1, remoteNode2, remoteNode3, localNode0)
    val statement = replicateAwareStatementFor(remoteNode3, localNode0)

    val plan = policy.newQueryPlan("superKeyspace", statement).toSeq.map(_.getAddress)

    plan.size should be(4)
    plan.take(2) should contain inOrderOnly(localNode0, remoteNode3)
    plan.takeRight(2) should contain only(remoteNode1, remoteNode2)
  }

  it should "use available hosts if there are no replicas supplied" in {
    val policy = policyForClusterOf(remoteNode1, remoteNode2, remoteNode3)
    val statement = replicateAwareStatementFor()

    val plan = policy.newQueryPlan("superKeyspace", statement).toSeq.map(_.getAddress)

    plan.size should be(3)
    plan.takeRight(3) should contain only(remoteNode1, remoteNode2, remoteNode3)
  }

  it should "use available hosts if there are no replicas supplied and local should be favored" in {
    val policy = policyForClusterOf(remoteNode1, remoteNode2, remoteNode3, localNode0)
    val statement = replicateAwareStatementFor()

    val plan = policy.newQueryPlan("superKeyspace", statement).toSeq.map(_.getAddress)

    plan.size should be(4)
    plan.head should be(localNode0)
    plan.takeRight(3) should contain only(remoteNode1, remoteNode2, remoteNode3)
  }

  it should "not use unavailable supplied replicas" in {
    val policy = policyForClusterOf(remoteNode1, remoteNode2)
    val statement = replicateAwareStatementFor(remoteNode3, localNode0)

    val plan = policy.newQueryPlan("superKeyspace", statement).toSeq.map(_.getAddress)

    plan.size should be(2)
    plan should contain only(remoteNode1, remoteNode2)
  }

  it should "not favor local host if is not present among supplied replicas" in {
    val policy = policyForClusterOf(remoteNode1, remoteNode2, localNode0)
    val statement = replicateAwareStatementFor(remoteNode2)

    val plan = policy.newQueryPlan("superKeyspace", statement).toSeq.map(_.getAddress)

    plan.size should be(3)
    plan.head should be(remoteNode2)
    plan.takeRight(2) should contain only(remoteNode1, localNode0)
  }
  */
}
