package com.datastax.spark.connector

import org.apache.spark.streaming.StreamingContext
 
package object streaming {

  implicit def toStreamingContextFunctions(ssc: StreamingContext): SparkContextFunctions =
    new StreamingContextFunctions(ssc)

}
