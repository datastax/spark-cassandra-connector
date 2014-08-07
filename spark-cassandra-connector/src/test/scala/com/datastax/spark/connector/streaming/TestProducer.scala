package com.datastax.spark.connector.streaming

import scala.concurrent.duration._
import akka.actor.{PoisonPill, Actor, ActorRef}

class TestProducer(data: Array[String], to: ActorRef) extends Counter {
  import scala.util.Random
  import context.dispatcher

  val rand = new Random()

  val task = context.system.scheduler.schedule(2.second, 1.millis) {
    if (count < scale) {  // we need this test to avoid generating more than 'scale' messages
      to ! makeMessage()
      increment()
    }
  }

  def receive: Actor.Receive = {
    case _ =>
  }

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    data(x) + data(2 - x)
  }
}

trait CounterFixture {
  val scale = 30
}

// CountDownLatch is not Serializable, can't use in stream so we do this.
trait Counter extends Actor with CounterFixture {

  var count = 0

  def increment(): Unit = {
    count += 1
    if (count == scale) self ! PoisonPill
  }
}