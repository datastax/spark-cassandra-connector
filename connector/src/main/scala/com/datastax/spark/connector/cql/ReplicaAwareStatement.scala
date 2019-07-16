/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.cql

import java.net.InetAddress

//TODO: remove this, as DSP-15202 states, this was created because it was not possible to create tokens to pass as arguments
// to set routingToken methods. Tokens are now easily createable, so this needs to go
/** Associates given statement with a set of replica addresses. It is meant to be interpreted by
  * [[LoadBalancingPolicy]] implementations, like [[LocalNodeFirstLoadBalancingPolicy]]. */
class ReplicaAwareStatement(
  val wrapped: Statement,
  val replicas: Set[InetAddress])
  extends StatementWrapper(wrapped) {
}
