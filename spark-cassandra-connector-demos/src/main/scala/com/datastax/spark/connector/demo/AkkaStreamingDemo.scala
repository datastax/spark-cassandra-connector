package com.datastax.spark.connector.demo

import scala.collection.immutable
import akka.actor._
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.streaming._

object AkkaStreamingDemo extends StreamingDemo {

  val system = ActorSystem("DemoApp")

  val guardian = system.actorOf(Props(new NodeGuardian(ssc, keyspaceName, tableName, data)), "node-guardian")

}

class Streamer extends TypedStreamingActor[String] with CounterActor {

  override def push(e: String): Unit = {
    super.push(e)
    increment()
  }
}

class NodeGuardian(ssc: StreamingContext, keyspaceName: String, tableName: String, data: immutable.Set[String])
  extends Actor with Logging {

  import scala.concurrent.duration._
  import akka.util.Timeout
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
  import com.datastax.spark.connector._
  import InternalStreamingEvent._
  import context.dispatcher

  implicit val timeout = Timeout(5.seconds)

  private val actorName = "stream"

  private val sas = SparkEnv.get.actorSystem

  private val path = ActorPath.fromString(s"$sas/user/Supervisor0/$actorName")

  context.system.registerOnTermination(shutdown())

  private val reporter = context.actorOf(Props(new Reporter(ssc, keyspaceName, tableName, data)), "reporter")

  private val stream = ssc.actorStream[String](Props[Streamer], actorName, StorageLevel.MEMORY_AND_DISK)

  /* Defines the work to do in the stream. */
  private val wc = stream.flatMap(_.split("\\s+"))
    .map(x => (x, 1))
    .reduceByKey(_ + _)
    .saveToCassandra("streaming_test", "words", SomeColumns("word", "count"), 1)

  ssc.start()
  log.info(s"Streaming context started.")


  for (actor <- sas.actorSelection(path).resolveOne()) {
    context.watch(actor)
    context.actorOf(Props(new Sender(data.toArray, actor)))
  }

  def receive: Actor.Receive = {
    case Terminated(ref) => reporter ! Report
    case Completed       => context.system.shutdown()
  }

  def shutdown(): Unit = {
    log.info(s"Stopping '$ssc' and shutting down.")
    ssc.stop()
  }

}
