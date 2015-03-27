package com.datastax.spark.connector.streaming

import akka.actor.{ActorSystem, Props, Terminated}
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition
import com.datastax.spark.connector.{RowsInBatch, SomeColumns}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.streaming.StreamingEvent.ReceiverStarted
import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.ConfigMap

class ActorStreamingSpec extends ActorSpec with CounterFixture with ImplicitSender {
  import com.datastax.spark.connector.testkit.TestEvent._

  /* Initializations - does not work in the actor test context in a static before() */
  CassandraConnector(SparkTemplate.defaultConf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_wordcount (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_join (word TEXT PRIMARY KEY, count COUNTER)")
    for (word <- data) {
      session.execute("UPDATE demo.streaming_join set count = count + 10 where word = ?", word.trim)
    }
    session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_join_output (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute("CREATE TABLE IF NOT EXISTS demo.dstream_join_output (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute("TRUNCATE demo.streaming_wordcount")
    session.execute("TRUNCATE demo.streaming_join_output")
    session.execute("TRUNCATE demo.dstream_join_output")
  }

  "actorStream" must {
    "write from the actor stream to cassandra table: demo.streaming_wordcount" in {

      val stream = ssc.actorStream[String](Props[TestStreamingActor], actorName, StorageLevel.MEMORY_AND_DISK)

      val wc = stream.flatMap(_.split("\\s+"))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("demo", "streaming_wordcount")

      // start the streaming context so the data can be processed and actor started
      ssc.start()

      system.eventStream.subscribe(self, classOf[StreamingEvent.ReceiverStarted])

      expectMsgPF(duration) { case ReceiverStarted(receiver) =>
        watch(receiver)
        system.actorOf(Props(new TestProducer(data.toArray, receiver)))
      }

      expectMsgPF(duration) { case Terminated(ref) =>
        val rdd = ssc.cassandraTable[WordCount]("demo", "streaming_wordcount")
        awaitCond(rdd.collect.nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2)
        rdd.collect.size should be (data.size)
      }
    }
    "be able to utilize joinWithCassandra during transforms " in {

      val stream = ssc.actorStream[String](Props[TestStreamingActor], actorName, StorageLevel.MEMORY_AND_DISK)
        .flatMap(_.split("\\s+"))

      val wc = stream
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("demo", "streaming_wordcount")

      val jc = stream
        .map(new Tuple1(_))
        .transform(rdd => rdd.joinWithCassandraTable("demo", "streaming_join"))
        .map(_._2)
        .saveToCassandra("demo", "streaming_join_output")

      ssc.start()

      system.eventStream.subscribe(self, classOf[StreamingEvent.ReceiverStarted])

      expectMsgPF(duration) { case ReceiverStarted(receiver) =>
        watch(receiver)
        system.actorOf(Props(new TestProducer(data.toArray, receiver)))
      }

      expectMsgPF(duration) { case Terminated(ref) =>
        val rdd = ssc.cassandraTable[WordCount]("demo", "streaming_join_output")
        awaitCond(rdd.collect.nonEmpty && rdd.collect.size == data.size)
        ssc.sparkContext.cassandraTable("demo", "streaming_join_output").collect.size should be(data.size)
      }
    }
    "be able to utilize joinWithCassandra and repartitionByCassandraTable on a Dstream " in {

      val stream = ssc.actorStream[String](Props[TestStreamingActor], actorName, StorageLevel.MEMORY_AND_DISK)
        .flatMap(_.split("\\s+"))

      val wc = stream
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("demo", "streaming_wordcount")

      val jcRepart = stream
        .map(new Tuple1(_))
        .repartitionByCassandraReplica("demo","streaming_join")

      val conn = CassandraConnector(ssc.sparkContext.getConf)

      jcRepart.foreachRDD(rdd => rdd.partitions.foreach {
        case e: EndpointPartition =>
          conn.hosts should contain(e.endpoints.head)
        case _ =>
          fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
      })

      jcRepart.joinWithCassandraTable("demo","streaming_join")
        .map(_._2)
        .saveToCassandra("demo", "dstream_join_output")

      ssc.start()

      system.eventStream.subscribe(self, classOf[StreamingEvent.ReceiverStarted])

      expectMsgPF(duration) { case ReceiverStarted(receiver) =>
        watch(receiver)
        system.actorOf(Props(new TestProducer(data.toArray, receiver)))
      }

      expectMsgPF(duration) { case Terminated(ref) =>
        val rdd = ssc.cassandraTable[WordCount]("demo", "dstream_join_output")
        awaitCond(rdd.collect.nonEmpty && rdd.collect.size == data.size)
        ssc.sparkContext.cassandraTable("demo", "dstream_join_output").collect.size should be(data.size)
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

abstract class ActorSpec(var ssc: StreamingContext, _system: ActorSystem)
  extends TestKit(_system) with StreamingSpec {

  def this() = this (new StreamingContext(SparkTemplate.useSparkConf(), Milliseconds(300)), SparkTemplate.actorSystem)

  before {
    //We can't re-use streaming contexts
    ssc = new StreamingContext(sc, Milliseconds(300))
  }
}




