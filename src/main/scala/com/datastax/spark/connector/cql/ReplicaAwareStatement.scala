/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.{Statement, StatementWrapper}

/** Associates given statement with a set of replica addresses. It is meant to be interpreted by
  * [[LoadBalancingPolicy]] implementations, like [[LocalNodeFirstLoadBalancingPolicy]]. */
class ReplicaAwareStatement(
  val wrapped: Statement,
  val replicas: Set[InetAddress])
  extends StatementWrapper(wrapped) {
}
