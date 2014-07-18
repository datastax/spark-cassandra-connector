package com.datastax.spark.connector.streaming

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SparkContextFixture, AbstractSpec}

/**
 * Usages: Create the [[org.apache.spark.streaming.StreamingContext]] then write async to the stream.
 *
 * val ssc = new StreamingContext(conf, Milliseconds(500))
 *
 * Akka
 * {{{
 *   val stream = ssc.actorStream[String](Props[SimpleActor], actorName, StorageLevel.MEMORY_AND_DISK)
 * }}}
 *
 * On upgrade examples:
 * Kafka
 * {{{
 *   val stream: ReceiverInputDStream[(String, String)] =
 *     KafkaUtils.createStream(ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)
 * }}}
 *
 * ZeroMQ
 * {{{
 *   val stream: ReceiverInputDStream[String] = ZeroMQUtils.createStream(ssc, publishUrl, subscribe, bytesToObjects)
 * }}}
 *
 * Twitter
 * {{{
 *   val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)
 * }}}
 *
 * etc.
 */
trait StreamingSpec extends AbstractSpec with SparkContextFixture {

  val next = new AtomicInteger(0)

  /* Keep in proportion with the above event num - not too long for CI without
* long-running sbt task exclusion.  */
  val events = 50

  val duration = 60.seconds

  /* Initializations */
  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS streaming_test.words (word TEXT PRIMARY KEY, count INT)")
    session.execute("TRUNCATE streaming_test.words")
  }
}
