package com.datastax.spark.connector

import scala.collection.immutable
import scala.concurrent.duration._
import akka.util.Timeout

package object testkit {

  final val DefaultHost = "127.0.0.1"

  implicit val DefaultTimeout = Timeout(5.seconds)

  val data = immutable.Set("words ", "may ", "count ")

  val actorName = "my-actor"

}
