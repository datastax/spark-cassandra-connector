package com.datastax.driver.spark.rdd

import com.datastax.driver.spark.rdd.dht.{Token, TokenRange}

/** Splits a token range into smaller sub-ranges,
  * each with the desired approximate number of rows. */
trait TokenRangeSplitter[V, T <: Token[V]] {

  /** Splits given token range into n equal sub-ranges. */
  def split(range: TokenRange[V, T], splitSize: Long): Seq[TokenRange[V, T]]
}





