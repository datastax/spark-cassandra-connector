package com.datastax.spark.connector.rdd.partitioner.dht

import java.net.InetAddress


case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, replicas: Set[InetAddress], rowCount: Option[Long]) {

  def isWrapAround: Boolean =
    start >= end

  def unwrap(implicit tokenFactory: TokenFactory[V, T]): Seq[TokenRange[V, T]] = {
    val minToken = tokenFactory.minToken
    if (isWrapAround)
      Seq(
        TokenRange(start, minToken, replicas, rowCount.map(_ / 2)),
        TokenRange(minToken, end, replicas, rowCount.map(_ / 2)))
    else
      Seq(this)
  }
}