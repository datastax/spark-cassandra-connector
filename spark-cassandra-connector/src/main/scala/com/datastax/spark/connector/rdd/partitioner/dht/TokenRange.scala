package com.datastax.spark.connector.rdd.partitioner.dht

import java.net.InetAddress

case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, replicas: Set[InetAddress], tokenFactory: TokenFactory[V, T]) {

  private[partitioner] lazy val rangeSize = tokenFactory.distance(start, end)

  private[partitioner] lazy val ringFraction = tokenFactory.ringFraction(start, end)

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
        TokenRange(start, minToken, replicas, tokenFactory),
        TokenRange(minToken, end, replicas, tokenFactory))
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

