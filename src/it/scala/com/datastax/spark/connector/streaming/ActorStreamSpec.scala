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

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Props
import akka.testkit.TestKit
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.demo.DemoApp.WordCount
import com.datastax.spark.connector.util.{CassandraServer, SparkServer}
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.duration._

trait ActorStreamWriter extends WordSpecLike with Matchers with CassandraServer with BeforeAndAfter with SparkStreamingSpecFixture with SparkServer {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(InetAddress.getByName("127.0.0.1"))

  var ssc: StreamingContext = null

  /* Keep in proportion with the above event num - not too long for CI without
  * long-running sbt task exclusion.  */
  val events = 100

  val duration = 30.seconds

  before {
    /* Initializations */
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count INT)")
      session.execute("TRUNCATE streaming_test.words")
    }

    ssc = new StreamingContext(sc, Milliseconds(500))
  }

  after {
    // Spark Context is shared among all integration test so we don't want to stop it here
    ssc.stop(stopSparkContext = false)
  }

}

class ActorStreamWriteReadSpec extends TestKit(SparkServer.actorSystem) with ActorStreamWriter {

  "actorStream" must {
    "write from the actor stream to cassandra table: streaming_test.words" in {
      import system.dispatcher

      val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)

      val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .foreachRDD(rdd => {
          rdd.saveToCassandra("streaming_test", "words", Seq("word", "count"))
      })

      ssc.start()

      val future = system.actorSelection(s"$system/user/Supervisor0/$actorName").resolveOne()
      awaitCond(future.isCompleted)
      for (actor <- future) system.actorOf(Props(new TestProducer(data.toArray, actor, events)))

      Thread.sleep(duration.toMillis) // a random test point to stop at and do assertions

      ssc.cassandraTable("streaming_test", "words").toArray().foreach(println)

      val rdd = ssc.cassandraTable[WordCount]("streaming_test", "words").select("word", "count")
      rdd.toArray().map(_.word).toSet should be (Set("words", "may", "count"))
      rdd.toArray().map(_.count).sum should be (2 * events)
    }
  }

}

