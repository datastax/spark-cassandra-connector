package com.datastax.spark.connector.streaming

import akka.actor.Actor
import com.datastax.spark.connector.SparkContextFunctions
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receivers.Receiver

/** Provides Cassandra-specific methods on [[StreamingContext]].
  * @param ssc the Spark Streaming context
  */
class StreamingContextFunctions (ssc: StreamingContext) extends SparkContextFunctions(ssc.sparkContext) {
  import java.io.{ Serializable => JSerializable }
  import scala.reflect.ClassTag

  override def cassandraTable[T <: JSerializable : ClassTag : RowReaderFactory](keyspace: String, table: String): CassandraStreamingRDD[T] =
    new CassandraStreamingRDD[T](ssc, keyspace, table)

}

/** Simple [[Actor]] mixin to implement further with Spark 1.0.1 upgrade. */
trait SparkStreamingActor extends Actor with Receiver


