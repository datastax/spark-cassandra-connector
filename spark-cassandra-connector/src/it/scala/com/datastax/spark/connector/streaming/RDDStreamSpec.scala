package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition
import com.datastax.spark.connector.testkit._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.streaming.Duration._
import org.scalatest.time.SpanSugar._

import scala.collection.mutable.Queue
import scala.util.Random

class RDDStreamingSpec
  extends SparkCassandraITFlatSpecBase
  with Eventually
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  import com.datastax.spark.connector.testkit.TestEvent._

  useCassandraConfig(Seq("cassandra-default.yaml.template"))

  override def beforeAll() {
    CassandraConnector(SparkTemplate.defaultConf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_wordcount (word TEXT PRIMARY KEY, count COUNTER)")
      session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_join (word TEXT PRIMARY KEY, count COUNTER)")
      for (d <- dataSeq; word <- d) {
        session.execute("UPDATE demo.streaming_join set count = count + 10 where word = ?", word
          .trim)
      }
      session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_join_output (word TEXT PRIMARY KEY, count COUNTER)")
      session.execute("CREATE TABLE IF NOT EXISTS demo.dstream_join_output (word TEXT PRIMARY KEY, count COUNTER)")

      session.execute("TRUNCATE demo.streaming_wordcount")
      session.execute("TRUNCATE demo.streaming_join_output")
      session.execute("TRUNCATE demo.dstream_join_output")
    }
  }

  val r = new Random()
  // Build 4 400 Element RDDs to use as a DStream
  val dataRDDs = new Queue[RDD[String]]()

  override def beforeEach() {
    while (dataRDDs.size > 0) dataRDDs.dequeue
    for (rddNum <- 1 to 4) {
      dataRDDs.enqueue(sc.parallelize((1 to 400).map(item => data(r.nextInt(data.size)))))
    }
  }

  def withStreamingContext(test: StreamingContext => Any) = {
    val ssc = new StreamingContext(sc, Milliseconds(200))
    try{
      test(ssc)
    }
    finally (ssc.stop(false, true))
  }

  "RDDStream" should "write from the stream to cassandra table: demo.streaming_wordcount" in withStreamingContext{ ssc =>
    val stream = ssc.queueStream[String](dataRDDs)

    val wc = stream
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .saveToCassandra("demo", "streaming_wordcount")

    // start the streaming context so the data can be processed and actor started
    ssc.start()
    eventually ( timeout(1 seconds) ){dataRDDs.isEmpty}

    eventually ( timeout(5 seconds)) {
      val rdd = ssc.cassandraTable[WordCount]("demo", "streaming_wordcount")
      val result = rdd.collect
      result.nonEmpty should be(true)
      result.size should be(data.size)
    }
  }

   it should "be able to utilize joinWithCassandra during transforms " in withStreamingContext { ssc =>
     val stream = ssc.queueStream[String](dataRDDs)

     val wc = stream
       .map(x => (x, 1))
       .reduceByKey(_ + _)
       .saveToCassandra("demo", "streaming_wordcount")

     stream
       .map(new Tuple1(_))
       .transform(rdd => rdd.joinWithCassandraTable("demo", "streaming_join"))
       .map(_._2)
       .saveToCassandra("demo", "streaming_join_output")

     ssc.start()

     eventually ( timeout(1 seconds) ){dataRDDs.isEmpty}

     eventually ( timeout(5 seconds)) {
       val rdd = ssc.cassandraTable[WordCount]("demo", "streaming_join_output")
       val result = rdd.collect
       result.nonEmpty should be(true)
       result.size should be(data.size)
       rdd.collect.nonEmpty && rdd.collect.size == data.size
       ssc.sparkContext.cassandraTable("demo", "streaming_join_output").collect.size should be(data.size)
     }
   }

    it should "be able to utilize joinWithCassandra and repartitionByCassandraTable on a Dstream " in withStreamingContext{ ssc =>
      val stream = ssc.queueStream[String](dataRDDs)

      val wc = stream
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .saveToCassandra("demo", "streaming_wordcount")

      val jcRepart = stream
        .map(new Tuple1(_))
        .repartitionByCassandraReplica("demo", "streaming_join")

      val conn = CassandraConnector(ssc.sparkContext.getConf)

      jcRepart.foreachRDD(rdd => rdd.partitions.foreach {
        case e: EndpointPartition =>
          conn.hosts should contain(e.endpoints.head)
        case _ =>
          fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
      })

      jcRepart.joinWithCassandraTable("demo", "streaming_join")
        .map(_._2)
        .saveToCassandra("demo", "dstream_join_output")

      ssc.start()

      eventually ( timeout(1 seconds) ){dataRDDs.isEmpty}

      eventually ( timeout(5 seconds)) {
        val rdd = ssc.cassandraTable[WordCount]("demo", "dstream_join_output")
        val result = rdd.collect
        result should have size (data.size)
        ssc.sparkContext.cassandraTable("demo", "dstream_join_output").collect.size should be(data.size)
      }
    }
}
