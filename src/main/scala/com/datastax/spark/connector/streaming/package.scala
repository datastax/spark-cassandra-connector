package com.datastax.spark.connector

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

package object streaming {

  implicit def toStreamingContextFunctions(ssc: StreamingContext): SparkContextFunctions =
    new StreamingContextFunctions(ssc)

  implicit def toDStreamFunctions[T: ClassTag](ds: DStream[T]): DStreamFunctions[T] =
    new DStreamFunctions[T](ds)

}
