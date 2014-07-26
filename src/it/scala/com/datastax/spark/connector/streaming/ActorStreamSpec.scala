package com.datastax.spark.connector.streaming

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.demo.DemoApp.WordCount
import com.datastax.spark.connector.util.{CassandraServer, SparkServer}
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class ActorStreamingSpec extends ActorSpec {

  /* Initializations - does not work in the actor test context in a static before() */
  CassandraConnector(SparkServer.conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute("TRUNCATE streaming_test.words")
  }

  "actorStream" must {
    "write from the actor stream to cassandra table: streaming_test.words" in {
      val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)

      val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("streaming_test", "words", Seq("word", "count"))

      ssc.start()

      import system.dispatcher
      val future = system.actorSelection(s"$system/user/Supervisor0/$actorName").resolveOne()
      awaitCond(future.isCompleted)
      for (actor <- future) system.actorOf(Props(new TestProducer(data.toArray, actor, events)))
      Thread.sleep(duration.toMillis)

      val rdd = ssc.cassandraTable[WordCount]("streaming_test", "words").select("word", "count")
      rdd.map(_.count).reduce(_ + _) should be (events * 2)
      rdd.toArray.size should be (data.size)
    }
  }
}

abstract class ActorSpec(val ssc: StreamingContext, _system: ActorSystem) extends TestKit(_system) with StreamingSpec
  with CassandraServer {
  def this() = this (new StreamingContext(SparkServer.sc, Milliseconds(300)), SparkEnv.get.actorSystem)

 useCassandraConfig("cassandra-default.yaml.template")

  after {
    // Spark Context is shared among all integration test so we don't want to stop it here
    ssc.stop(stopSparkContext = false)
  }
}




