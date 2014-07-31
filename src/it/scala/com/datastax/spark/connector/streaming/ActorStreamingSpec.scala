package com.datastax.spark.connector.streaming

import akka.actor.{Terminated, ActorSystem, Props}
import akka.testkit.TestKit
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.testkit._

class ActorStreamingSpec extends ActorSpec with CounterFixture {
  import TestEvent._

  /* Initializations - does not work in the actor test context in a static before() */
  CassandraConnector(SparkServer.conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute("TRUNCATE streaming_test.words")
  }

  "actorStream" must {
    "write from an actor stream to a cassandra table and read what was written from cassandra" in {

      val stream = ssc.actorStream[String](Props[SimpleStreamingActor], actorName, StorageLevel.MEMORY_AND_DISK)

      val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("streaming_test", "words", Seq("word", "count"), Some(1))

      ssc.start()

      import system.dispatcher
      val future = system.actorSelection(s"$system/user/Supervisor0/$actorName").resolveOne()
      awaitCond(future.isCompleted)
      for (actor <- future) {
        watch(actor)
        system.actorOf(Props(new TestProducer(data.toArray, actor)))
      }

      expectMsgPF(duration) { case Terminated(ref) =>
        val rdd = ssc.cassandraTable[WordCount]("streaming_test", "words").select("word", "count")
        awaitCond(rdd.map(_.count).reduce(_ + _) == scale * 2)
        rdd.toArray().length should be (data.size)
      }
    }
  }
}

abstract class ActorSpec(val ssc: StreamingContext, _system: ActorSystem)
  extends TestKit(_system) with StreamingSpec {

  def this() = this(new StreamingContext(SparkServer.sc, Milliseconds(300)), SparkEnv.get.actorSystem)

}

