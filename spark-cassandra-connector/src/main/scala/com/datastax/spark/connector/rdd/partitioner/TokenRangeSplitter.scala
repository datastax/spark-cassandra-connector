package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenRange}

/** Splits a token range into smaller sub-ranges,
  * each with the desired approximate number of rows. */
trait TokenRangeSplitter[V, T <: Token[V]] {

  /** Splits given token range into n equal sub-ranges.
    * @param splitSize requested sub-split size, given in the same units as `dataSize`
    * @param numPartitions if set, the exact number of ranges are returned. splitSize is ignored
    */
  def split(range: TokenRange[V, T], splitSize: Long, numPartitions: Option[Int] = None): Seq[TokenRange[V, T]]
}
