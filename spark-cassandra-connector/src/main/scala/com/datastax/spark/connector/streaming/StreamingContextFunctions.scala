package com.datastax.spark.connector.streaming

import akka.actor.Actor
import com.datastax.spark.connector.SparkContextFunctions
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver
import org.apache.spark.streaming.receiver.ActorHelper

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

/** Simple akka.actor.Actor mixin to implement further with Spark 1.0.1 upgrade. */
trait SparkStreamingActor extends Actor with ActorHelper

abstract class TypedStreamingActor[T : ClassTag] extends SparkStreamingActor {

  def receive: Actor.Receive = {
    case e: T => push(e)
  }

  def push(event: T): Unit =
    store(event) 

}

