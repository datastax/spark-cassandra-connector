package com.datastax.spark.connector.rdd.partitioner.dht

import java.net.InetAddress


case class CassandraNode(rpcAddress: InetAddress, localAddress: InetAddress) {
  def allAddresses = Set(rpcAddress, localAddress)
}

object CassandraNode {
  implicit def ordering: Ordering[CassandraNode] = Ordering.by(_.rpcAddress.toString)
}

case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, endpoints: Set[CassandraNode], rowCount: Option[Long]) {

  def isWrapAround: Boolean =
    start >= end

  def unwrap(implicit tokenFactory: TokenFactory[V, T]): Seq[TokenRange[V, T]] = {
    val minToken = tokenFactory.minToken
    if (isWrapAround)
      Seq(
        TokenRange(start, minToken, endpoints, rowCount.map(_ / 2)),
        TokenRange(minToken, end, endpoints, rowCount.map(_ / 2)))
    else
      Seq(this)
  }
}