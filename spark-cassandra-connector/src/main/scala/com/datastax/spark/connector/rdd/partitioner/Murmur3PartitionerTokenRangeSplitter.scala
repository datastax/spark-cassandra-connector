package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{LongToken, TokenFactory, TokenRange}

/** Fast token range splitter assuming that data are spread out evenly in the whole range.
  * @param dataSize estimate of the size of the data in the whole ring */
class Murmur3PartitionerTokenRangeSplitter(dataSize: Long)
  extends TokenRangeSplitter[Long, LongToken] {

  private val tokenFactory =
    TokenFactory.Murmur3TokenFactory

  private type TR = TokenRange[Long, LongToken]

  /** Splits the token range uniformly into sub-ranges.
    * @param splitSize requested sub-split size, given in the same units as `dataSize` */
  def split(range: TR, splitSize: Long): Seq[TR] = {
    val rangeSize = dataSize * tokenFactory.ringFraction(range.start, range.end)
    val rangeTokenCount = tokenFactory.distance(range.start, range.end)
    val n = math.max(1, math.round(rangeSize / splitSize).toInt)

    val left = range.start.value
    val right = range.end.value
    val splitPoints =
      (for (i <- 0 until n) yield left + (rangeTokenCount * i / n).toLong) :+ right

    for (Seq(l, r) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange[Long, LongToken](
        new LongToken(l),
        new LongToken(r),
        range.replicas,
        Some((rangeSize / n).toInt))
  }
}
