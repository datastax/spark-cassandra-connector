package com.datastax.spark.connector.streaming

import akka.actor.{ActorSystem, Props, Terminated}
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.spark.connector.streaming.StreamingEvent.ReceiverStarted
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector.embedded._

class ActorStreamingSpec extends ActorSpec with CounterFixture with ImplicitSender {
  import TestEvent._

  /* Initializations - does not work in the actor test context in a static before() */
  CassandraConnector(SparkTemplate.conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute("TRUNCATE streaming_test.words")
  }

  "actorStream" must {
    "write from the actor stream to cassandra table: streaming_test.words" in {

      val stream = ssc.actorStream[String](Props[TestStreamingActor], actorName, StorageLevel.MEMORY_AND_DISK)

      val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("streaming_test", "words", SomeColumns("word", "count"), 1)

      // start the streaming context so the data can be processed and actor started
      ssc.start

      system.eventStream.subscribe(self, classOf[StreamingEvent.ReceiverStarted])

      expectMsgPF(duration) { case ReceiverStarted(receiver) =>
        watch(receiver)
        system.actorOf(Props(new TestProducer(data.toArray, receiver)))
      }

      expectMsgPF(duration) { case Terminated(ref) =>
        val rdd = ssc.cassandraTable[WordCount]("streaming_test", "words").select("word", "count")
        awaitCond(rdd.collect.nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2)
        rdd.collect.length should be (data.size)
      }
    }
  }
}

/** A very basic Akka actor which streams `String` event data to spark. */
class TestStreamingActor extends TypedStreamingActor[String] with Counter {

  override def push(e: String): Unit = {
    super.push(e)
    increment()
  }
}

abstract class ActorSpec(val ssc: StreamingContext, _system: ActorSystem)
  extends TestKit(_system) with StreamingSpec {

  def this() = this (new StreamingContext(SparkTemplate.sc, Milliseconds(300)), SparkEnv.get.actorSystem)

}




