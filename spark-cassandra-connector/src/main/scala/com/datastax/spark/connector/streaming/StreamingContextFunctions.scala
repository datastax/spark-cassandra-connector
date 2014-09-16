package com.datastax.spark.connector.streaming

import akka.actor.{ActorRef, Actor}
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.receiver.ActorHelper
import com.datastax.spark.connector.SparkContextFunctions
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `org.apache.spark.streaming.StreamingContext`.
  * @param ssc the Spark Streaming context
  */
class StreamingContextFunctions (ssc: StreamingContext) extends SparkContextFunctions(ssc.sparkContext) {
  import java.io.{ Serializable => JSerializable }
  import scala.reflect.ClassTag

  override def cassandraTable[T <: JSerializable : ClassTag : RowReaderFactory](keyspace: String, table: String): CassandraStreamingRDD[T] =
    new CassandraStreamingRDD[T](ssc, keyspace, table)

}

/** Simple akka.actor.Actor mixin. */
trait SparkStreamingActor extends Actor with ActorHelper with Logging {

  override def preStart(): Unit = {
    log.info(s"${self.path} starting.")
    context.system.eventStream.publish(StreamingEvent.ReceiverStarted(self))
  }
}

abstract class TypedStreamingActor[T : ClassTag] extends SparkStreamingActor {

  def receive: Actor.Receive = {
    case e: T => push(e)
  }

  def push(event: T): Unit =
    store(event)
}

/** Simple StreamingListener. Currently just used to listen for initialization of a receiver.
  * Implement further to access information about an ongoing streaming computation.*/
class SparkStreamingListener[T: ClassTag] extends StreamingListener {
  import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
  import java.util.concurrent.atomic.AtomicBoolean

  private val listenerInitialized = new AtomicBoolean()

  def initialized: Boolean = listenerInitialized.get

  /** Called when a receiver has been started */
  override def onReceiverStarted(started: StreamingListenerReceiverStarted): Unit =
    listenerInitialized.set(true)

}

object StreamingEvent {
  /** Base marker for Receiver events */
  sealed trait ReceiverEvent extends Serializable

  /**
   * @param actor the receiver actor
   */
  case class ReceiverStarted(actor: ActorRef) extends ReceiverEvent
}