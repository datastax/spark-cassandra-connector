package com.datastax.spark.connector.demo.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkEnv, SparkConf, Logging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.streaming.kafka.embedded._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.util.Assertions

object KafkaStreamingDemo extends Assertions with Logging {

  /* Initialize Akka, Cassandra and Spark settings. */
  val settings = new SparkCassandraSettings()
  import settings._

  /** Configures Spark. */
  val sc = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setMaster(SparkMaster)
    .setAppName("Streaming Kafka App")

  /** Creates the keyspace and table in Cassandra. */
  CassandraConnector(sc).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS streaming_test.key_value (key VARCHAR PRIMARY KEY, value INT)")
    session.execute(s"TRUNCATE streaming_test.key_value")
  }

  private val topic = "topic1"

  /** Starts the Kafka broker. */
  lazy val kafka = new EmbeddedKafka

  def main(args: Array[String]) {

    /** Creates the Spark Streaming context. */
    val ssc =  new StreamingContext(sc, Seconds(2))
    SparkEnv.get.actorSystem.registerOnTermination(kafka.shutdown())

    val sent =  Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafka.createTopic(topic)
    kafka.produceAndSendMessage(topic, sent)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafka.kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)

    /* Defines the work to do in the stream. Placing the import here to explicitly show
     that this is where the implicits are used for the DStream's 'saveToCassandra' functions: */
    import com.datastax.spark.connector.streaming._

    stream.map { case (_, v) => v }
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .saveToCassandra("streaming_test", "key_value", SomeColumns("key", "value"), 1)

    ssc.start()

    val rdd = ssc.cassandraTable("streaming_test", "key_value").select("key", "value")
    import scala.concurrent.duration._
    awaitCond(rdd.collect.size == sent.size, 5.seconds)
    val rows = rdd.collect
    sent.forall { rows.contains(_)}

    log.info(s"Assertions successful, shutting down.")
    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }
}
