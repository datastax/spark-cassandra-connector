package com.datastax.spark.connector

import scala.collection.immutable
import scala.concurrent.duration._
import akka.util.Timeout

package object testkit {

  final val DefaultHost = "127.0.0.1"

  implicit val DefaultTimeout = Timeout(5.seconds)


  val dataSeq = Seq (
    immutable.Set("1first ", "1round ", "1words "),
    immutable.Set("2second ", "2round ", "2words "),
    immutable.Set("3third ", "3round ", "3words "),
    immutable.Set("4fourth ", "4round ", "4words ")
  )

  val data = dataSeq.head

  val actorName = "my-actor"

}
