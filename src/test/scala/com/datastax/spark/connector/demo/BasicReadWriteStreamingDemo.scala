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
package com.datastax.spark.connector.demo

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor._
import akka.testkit.TestKit
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkEnv
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

object BasicReadWriteStreamingDemo extends App with SparkContextFixture with AbstractSpec {

  import com.datastax.spark.connector.streaming._

  case class WordCount(word: String, count: Int)

  private val next = new AtomicInteger(0)

  /* Keep in proportion with the above event num - not too long for CI without
  * long-running sbt task exclusion.  */
  private val events = 20

  private val duration = 30.seconds

  private val ssc = new StreamingContext(SparkContextFixture.conf, Seconds(1))

  private val testkit = new TestKit(SparkEnv.get.actorSystem)
  import testkit._

  /* Initializations */
  CassandraConnector(ssc.sparkContext.getConf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count INT)")
    session.execute("TRUNCATE streaming_test.words")
  }

  private val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)

  private val wc = stream.flatMap(_.split("\\s+"))
    .map(x => (x, 1))
    .reduceByKey(_ + _)
    .foreachRDD(rdd => {
      next.getAndIncrement // just a test counter
      rdd.saveToCassandra("streaming_test", "words", Seq("word", "count"))
  })

  /** Start Spark streaming */
  ssc.start()

  import system.dispatcher
  private val future = system.actorSelection(s"$system/user/Supervisor0/$actorName").resolveOne()
  awaitCond(future.isCompleted)
  for (actor <- future) system.actorOf(Props(new TestProducer(data.toArray, actor, events)))

  awaitCond(next.get == events, duration) // a random test point to stop at and do assertions

  // Now read the table streaming_test.kv:
  private val rdd = ssc.cassandraTable[WordCount]("streaming_test", "words").select("word", "count")
  rdd.first.word should be ("words")
  rdd.first.count should be > (3)
  rdd.toArray.size should be (data.size)
  println(s"Cassandra Data:")
  rdd.toArray.foreach(println)

  ssc.stop(true)

  system.shutdown()
}