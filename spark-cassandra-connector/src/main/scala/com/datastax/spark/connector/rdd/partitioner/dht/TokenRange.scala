package com.datastax.spark.connector.rdd.partitioner.dht

import java.net.InetAddress

case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, endpoints: Set[InetAddress], rowCount: Option[Long]) {

  def contains(value: T): Boolean = {
    if (isWrapAround) {
      value > start || value <= end
    } else {
      value > start && value <= end
    }
  }


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