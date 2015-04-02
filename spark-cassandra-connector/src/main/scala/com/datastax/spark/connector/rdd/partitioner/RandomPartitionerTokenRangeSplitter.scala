package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{BigIntToken, TokenFactory, TokenRange}

import scala.math.BigDecimal.RoundingMode

/** Fast token range splitter assuming that data are spread out evenly in the whole range. */
class RandomPartitionerTokenRangeSplitter(cassandraPartitionsPerToken: Double) extends TokenRangeSplitter[BigInt, BigIntToken] {

  private val tokenFactory =
    TokenFactory.RandomPartitionerTokenFactory

  private def wrap(token: BigInt): BigInt = {
    val max = tokenFactory.maxToken.value
    if (token <= max) token else token - max
  }

  def split(range: TokenRange[BigInt, BigIntToken], splitSize: Long) = {
    val left = range.start.value
    val right = range.end.value
    val rangeSize = BigDecimal(tokenFactory.distance(range.start, range.end))
    val estimatedRows = rangeSize * cassandraPartitionsPerToken
    val n = math.max(1, (estimatedRows / splitSize).setScale(0, RoundingMode.HALF_UP).toInt)
    val splitPoints =
      (for (i <- 0 until n) yield wrap(left + (rangeSize * i.toDouble / n).toBigInt)) :+ right
    for (Seq(l, r) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange[BigInt, BigIntToken](
        new BigIntToken(l.bigInteger),
        new BigIntToken(r.bigInteger),
        range.replicas,
        Some((estimatedRows / n).toInt))
  }
}