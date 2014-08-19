package com.datastax.spark.connector.demo.streaming

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.{Actor, PoisonPill}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkEnv}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.demo.DemoApp

/**
 * Creates the `org.apache.spark.streaming.StreamingContext` then write async to the stream.
 * This is the base for all streaming demos.
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

  val ssc = new StreamingContext(sc, Milliseconds(300))

  lazy val sparkActorSystem = SparkEnv.get.actorSystem

}


/* Initializes Akka, Cassandra and Spark settings. */
final class SparkCassandraSettings(rootConfig: Config) {
  def this() = this(ConfigFactory.load)

  protected val config = rootConfig.getConfig("spark-cassandra")

  val SparkMaster: String = config.getString("spark.master")

  val SparkAppName: String = config.getString("spark.app.name")

  val SparkCleanerTtl: Int = config.getInt("spark.cleaner.ttl")

  val CassandraSeed: String = config.getString("spark.cassandra.connection.host")

  val CassandraKeyspace = config.getString("spark.cassandra.keyspace")

  val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")
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

/** When called upon, the Reporter starts a task which checks at regular intervals whether
  * the produced amount of data has all been written to Cassandra from the stream. This allows
  * the demo to stop on its own once this assertion is true. It will stop the task and ping
  * the `NodeGuardian`, its supervisor, of the `Completed` state.
  */
class Reporter(ssc: StreamingContext, keyspaceName: String, tableName: String, data: immutable.Set[String]) extends CounterActor  {
  import akka.actor.Cancellable
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
      if (rdd.collect.nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2) {
        context.become(done)
        self ! Completed
      }
    })
  }

  def complete(): Unit = {
    task map (_.cancel())
    val rdd = ssc.cassandraTable[WordCount](keyspaceName, tableName).select("word", "count")
    assert(rdd.collect.length == data.size)
    log.info(s"Saved data to Cassandra.")
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
trait KafkaStreamingDemo extends StreamingDemo

/**
 * TODO
 * ZeroMQ
 * {{{
 *   val stream: ReceiverInputDStream[String] = ZeroMQUtils.createStream(ssc, publishUrl, subscribe, bytesToObjects)
 * }}}
 */
trait ZeroMQStreamingDemo extends StreamingDemo
