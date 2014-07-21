/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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


