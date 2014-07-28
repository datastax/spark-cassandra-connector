package com.datastax.spark.connector.streaming

import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Random
import akka.actor.{PoisonPill, Actor, ActorRef}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

/** Extend (and augment) this fixture for the other input stream types by adding abstract class specs */
trait SparkStreamingFixture {

  implicit val DefaultTimeout = Timeout(5.seconds)

  val actorName = "my-actor"

  val data = immutable.Set("words ", "may ", "count ")

}

private[streaming] object TestEvent {

  case object Stop
  case object Completed
}

class TestProducer(data: Array[String], to: ActorRef, scale: Int) extends Actor {

  import context.dispatcher

  val rand = new Random()
  var count = 0

  val task = context.system.scheduler.schedule(2.second, 1.millis) {
    to ! makeMessage()
    count += 1
    // sent all events we want to send
    if (count == scale) self ! TestEvent.Stop
  }

  def receive: Actor.Receive = {
    case TestEvent.Stop =>
      // stop generating messages that go to the stream
      task.cancel()
      // initiate stop streaming so we can test assertions on the data
      to ! TestEvent.Completed
  }

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    data(x) + data(2 - x)
  }
}

/** A very basic Akka actor which streams String event data to spark.
  * TODO implement further. */
private [streaming] class SimpleActor extends SparkStreamingActor {
  def receive: Actor.Receive = {
    case e: String           => pushBlock(e)
    case TestEvent.Completed => self ! PoisonPill // to know when we can proceed with assertions via cassandra read
  }
}