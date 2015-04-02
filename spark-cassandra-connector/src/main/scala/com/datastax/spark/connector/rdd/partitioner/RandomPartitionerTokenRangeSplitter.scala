package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{BigIntToken, TokenFactory, TokenRange}


/** Fast token range splitter assuming that data are spread out evenly in the whole range.
  * @param dataSize estimate of the size of the data in the whole ring */
class RandomPartitionerTokenRangeSplitter(dataSize: Long)
  extends TokenRangeSplitter[BigInt, BigIntToken] {

  private val tokenFactory =
    TokenFactory.RandomPartitionerTokenFactory

  private def wrap(token: BigInt): BigInt = {
    val max = tokenFactory.maxToken.value
    if (token <= max) token else token - max
  }

  private type TR = TokenRange[BigInt, BigIntToken]

  /** Splits the token range uniformly into sub-ranges.
    * @param splitSize requested sub-split size, given in the same units as `dataSize` */
  def split(range: TR, splitSize: Long): Seq[TR] = {
    val rangeSize = dataSize * tokenFactory.ringFraction(range.start, range.end)
    val rangeTokenCount = tokenFactory.distance(range.start, range.end)
    val n = math.max(1, math.round(rangeSize / splitSize).toInt)

    val left = range.start.value
    val right = range.end.value
    val splitPoints =
      (for (i <- 0 until n) yield wrap(left + (rangeTokenCount * i / n))) :+ right

    for (Seq(l, r) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange[BigInt, BigIntToken](
        new BigIntToken(l.bigInteger),
        new BigIntToken(r.bigInteger),
        range.replicas,
        Some((rangeSize / n).toInt))
  }
}