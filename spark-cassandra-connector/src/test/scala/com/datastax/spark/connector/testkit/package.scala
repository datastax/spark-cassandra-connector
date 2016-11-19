package com.datastax.spark.connector

package object testkit {

  final val DefaultHost = "127.0.0.1"

  val dataSeq = Seq (
    Seq("1first", "1round", "1words"),
    Seq("2second", "2round", "2words"),
    Seq("3third", "3round", "3words"),
    Seq("4fourth", "4round", "4words")
  )

  val data = dataSeq.head

}
