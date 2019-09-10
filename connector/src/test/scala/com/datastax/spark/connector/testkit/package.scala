package com.datastax.spark.connector

import org.scalatest.{Matchers, WordSpecLike}

package object testkit {

  /** Basic unit test abstraction. */
  trait AbstractSpec extends WordSpecLike with Matchers

  val dataSeq = Seq (
    Seq("1first", "1round", "1words"),
    Seq("2second", "2round", "2words"),
    Seq("3third", "3round", "3words"),
    Seq("4fourth", "4round", "4words")
  )

  val data = dataSeq.head

}
