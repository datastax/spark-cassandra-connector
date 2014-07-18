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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.datastax.spark.connector.demo.DemoApp.WordCount

trait ActorStreamWriter extends AbstractSpec with SparkContextFixture {

  val next = new AtomicInteger(0)

  /* Keep in proportion with the above event num - not too long for CI without
  * long-running sbt task exclusion.  */
  val events = 20

  val duration = 30.seconds

  /* Initializations */
  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count INT)")
    session.execute("TRUNCATE streaming_test.words")
  }
}

object ActorSpec {
  val conf = new SparkConf(true)
    .set("spark.master", "local[12]")
    .set("spark.app.name", "Streaming Demo")
    .set("spark.cassandra.connection.host", "127.0.0.1")
}

class ActorSpec(val ssc: StreamingContext, _system: ActorSystem) extends TestKit(_system) with ActorStreamWriter {

  def this() = this (new StreamingContext(ActorSpec.conf, Seconds(1)), SparkEnv.get.actorSystem)

  def shutdown(): Unit = {
    ssc.stop(true)
    system.shutdown()
  }
}

class ActorStreamWriteReadSpec extends ActorSpec with AbstractSpec {
  import system.dispatcher

  val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)

  "actorStream" must {
    "write from the actor stream to cassandra table: streaming_test.words" in {
      val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .foreachRDD(rdd => {
          next.getAndIncrement // just a test counter
          rdd.saveToCassandra("streaming_test", "words", Seq("word", "count"))
      })

      ssc.start()

      val future = system.actorSelection(s"$system/user/Supervisor0/$actorName").resolveOne()
      awaitCond(future.isCompleted)
      for (actor <- future) system.actorOf(Props(new TestProducer(data.toArray, actor, events)))

      awaitCond(next.get == events, duration) // a random test point to stop at and do assertions
    }
    "read the cassandra table: streaming_test.words" in {
      val rdd = ssc.cassandraTable[WordCount]("streaming_test", "words").select("word", "count")
      rdd.first.word should be ("words")
      rdd.first.count should be > (3)
      rdd.toArray.size should be (data.size)

      shutdown()
    }
  }

}

