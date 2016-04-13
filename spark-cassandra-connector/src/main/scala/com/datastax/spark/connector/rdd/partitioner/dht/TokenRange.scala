package com.datastax.spark.connector.rdd.partitioner.dht

import java.net.InetAddress

case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, replicas: Set[InetAddress], dataSize: Long) {

  def isWrappedAround(implicit tf: TokenFactory[V, T]): Boolean =
    start >= end && end != tf.minToken

  def isFull(implicit tf: TokenFactory[V, T]): Boolean =
    start == end && end == tf.minToken

  def isEmpty(implicit tf: TokenFactory[V, T]): Boolean =
    start == end && end != tf.minToken

  def unwrap(implicit tf: TokenFactory[V, T]): Seq[TokenRange[V, T]] = {
    val minToken = tf.minToken
    if (isWrappedAround)
      Seq(
        TokenRange(start, minToken, replicas, dataSize / 2),
        TokenRange(minToken, end, replicas, dataSize / 2))
    else
      Seq(this)
  }

  def contains(token: T)(implicit tf: TokenFactory[V, T]): Boolean = {
    (end == tf.minToken && token > start
      || start == tf.minToken && token <= end
      || !isWrappedAround && token > start && token <= end
      || isWrappedAround && (token > start || token <= end))
  }
}

