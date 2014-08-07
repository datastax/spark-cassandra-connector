package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{ LongToken, TokenFactory, TokenRange }

import scala.math.BigDecimal.RoundingMode

/** Fast token range splitter assuming that data are spread out evenly in the whole range. */
class Murmur3PartitionerTokenRangeSplitter(rowsPerToken: Double) extends TokenRangeSplitter[Long, LongToken] {

  private val tokenFactory =
    TokenFactory.Murmur3TokenFactory

  def split(range: TokenRange[Long, LongToken], splitSize: Long) = {
    val left = range.start.value
    val right = range.end.value
    val rangeSize =
      if (right > left) BigDecimal(right) - BigDecimal(left)
      else BigDecimal(right) - BigDecimal(left) + BigDecimal(tokenFactory.totalTokenCount)
    val estimatedRows = rangeSize * rowsPerToken
    val n = math.max(1, (estimatedRows / splitSize).setScale(0, RoundingMode.HALF_UP).toInt)
    val splitPoints =
      (for (i <- 0 until n) yield left + (rangeSize * i.toDouble / n).toLong) :+ right
    for (Seq(l, r) <- splitPoints.sliding(2).toSeq) yield new TokenRange[Long, LongToken](
      new LongToken(l),
      new LongToken(r),
      range.endpoints,
      Some((estimatedRows / n).toInt))
  }
}
