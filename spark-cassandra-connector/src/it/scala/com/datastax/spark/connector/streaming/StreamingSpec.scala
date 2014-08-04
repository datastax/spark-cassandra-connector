package com.datastax.spark.connector.streaming

import scala.concurrent.duration._
import akka.actor.{PoisonPill, Actor, ActorRef}
import com.datastax.spark.connector.testkit._

/**
 * Usages: Create the [[org.apache.spark.streaming.StreamingContext]] then write async to the stream.
 *
 * val ssc = new StreamingContext(conf, Milliseconds(500))
 *
 * Akka
 * {{{
 *   val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)
 * }}}
 *
 * On upgrade examples:
 * Kafka
 * {{{
 *   val stream: ReceiverInputDStream[(String, String)] =
 *     KafkaUtils.createStream(ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)
 * }}}
 *
 * ZeroMQ
 * {{{
 *   val stream: ReceiverInputDStream[String] = ZeroMQUtils.createStream(ssc, publishUrl, subscribe, bytesToObjects)
 * }}}
 *
 * Twitter
 * {{{
 *   val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)
 * }}}
 *
 * etc.
 */
trait StreamingSpec extends AbstractSpec with CassandraServer with SparkCassandraFixture {
  import org.apache.spark.streaming.StreamingContext

  val duration = 10.seconds

  useCassandraConfig("cassandra-default.yaml.template")

  def ssc: StreamingContext

  after {
    // Spark Context is shared among all integration test so we don't want to stop it here
    ssc.stop(stopSparkContext = false)
  }
}

/** A very basic Akka actor which streams `String` event data to spark. */
private [streaming] class SimpleStreamingActor extends SparkStreamingActor with Counter {

  def receive: Actor.Receive = {
    case e: String => push(e)
  }

  def push(e: String): Unit = {
    pushBlock(e)
    increment()
  }
}

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
