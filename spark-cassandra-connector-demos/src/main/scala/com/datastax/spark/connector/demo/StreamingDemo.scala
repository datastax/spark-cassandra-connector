package com.datastax.spark.connector.demo

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.{PoisonPill, Actor, ActorRef}
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * Creates the [[org.apache.spark.streaming.StreamingContext]] then write async to the stream.
 */
trait StreamingDemo extends DemoApp {

  val keyspaceName = "streaming_test"

  val tableName = "words"

  val data = immutable.Set("words ", "may ", "count ")

  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspaceName WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspaceName.$tableName (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute(s"TRUNCATE $keyspaceName.$tableName")
  }

  lazy val ssc = new StreamingContext(sc, Milliseconds(300))

  lazy val sparkActorSystem = SparkEnv.get.actorSystem

}

trait CounterActor extends Actor  with Logging {

  protected val scale = 30

  private var count = 0

  protected def increment(): Unit = {
    count += 1
    if (count == scale) self ! PoisonPill
  }
}

private[demo] object InternalStreamingEvent {
  sealed trait Status
  case class Pushed(data: AnyRef) extends Status
  case object Completed extends Status
  case object Report extends Status
  case class WordCount(word: String, count: Int)
}

/** Generates and sends messages based on input `data`. */
class Sender(val data: Array[String], val to: ActorRef) extends Actor {
  import context.dispatcher

  private val rand = new scala.util.Random()

  val task = context.system.scheduler.schedule(2.second, 1.millis) {
    to ! createMessage()
  }

  override def postStop(): Unit = task.cancel()

  def createMessage(): String = {
    val x = rand.nextInt(3)
    data(x) + data(2 - x)
  }

  def receive: Actor.Receive = {
    case _ =>
  }
}

class Reporter(ssc: StreamingContext, keyspaceName: String, tableName: String, data: immutable.Set[String]) extends CounterActor  {
  import akka.actor.Cancellable
  import com.datastax.spark.connector._
  import com.datastax.spark.connector.streaming._
  import InternalStreamingEvent._
  import context.dispatcher

  private var task: Option[Cancellable] = None

  def receive: Actor.Receive = {
    case Report => report()
  }

  def done: Actor.Receive = {
    case Completed => complete()
  }

  def report(): Unit = {
    task = Some(context.system.scheduler.schedule(Duration.Zero, 1.millis) {
      val rdd = ssc.cassandraTable[WordCount](keyspaceName, tableName).select("word", "count")
      if (rdd.toArray().nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2) {
        context.become(done)
        self ! Completed
      }
    })
  }

  def complete(): Unit = {
    task map (_.cancel())
    val rdd = ssc.cassandraTable[WordCount](keyspaceName, tableName).select("word", "count")
    assert(rdd.toArray().length == data.size)
    log.info(s"Saved '${rdd.toArray()}' to Cassandra.")
    context.parent ! Completed
  }
}

/**
 * TODO
 * {{{
 *   val stream: ReceiverInputDStream[(String, String)] =
 *     KafkaUtils.createStream(ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)
 * }}}
 */
trait KhafkaStreamingDemo extends StreamingDemo

/**
 * TODO
 * ZeroMQ
 * {{{
 *   val stream: ReceiverInputDStream[String] = ZeroMQUtils.createStream(ssc, publishUrl, subscribe, bytesToObjects)
 * }}}
 */
trait ZeroMQStreamingDemo extends StreamingDemo
