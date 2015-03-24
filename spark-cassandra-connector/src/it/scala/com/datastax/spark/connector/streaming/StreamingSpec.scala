package com.datastax.spark.connector.streaming

import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector.embedded._
import org.scalatest.{ConfigMap, BeforeAndAfterAll}

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
trait StreamingSpec extends AbstractSpec with SharedEmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {
  import org.apache.spark.streaming.StreamingContext
  import scala.concurrent.duration._

  val duration = 10.seconds

  useCassandraConfig(Seq("cassandra-default.yaml.template"))

  def ssc: StreamingContext

  after {
    // Spark Context is shared among all integration test so we don't want to stop it here
    ssc.stop(stopSparkContext = false, stopGracefully = true)
  }

  override def afterAll(configMap: ConfigMap) {
    if (ssc.sparkContext != null) {
      ssc.sparkContext.stop()
    }
  }

}