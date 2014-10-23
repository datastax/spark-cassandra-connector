package com.datastax.spark.connector.embedded

import java.util.concurrent.CountDownLatch
import java.util.Properties
import java.util.concurrent.Executors

import scala.collection.JavaConversions._
import kafka.consumer.ConsumerConfig

/** The KafkaConsumer is a very simple consumer of a single Kafka topic.
  * This is a helpful utility for IT tests to insure data is getting published to Kafka
  * for streaming ingestion upstream.
  */
class KafkaConsumer(zookeeper: String, topic: String, groupId: String, numThreads: Int, latch: CountDownLatch) {

  val  consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig)

  // dude, that's dark:
  val mapForStreams = mapAsJavaMap(Map(topic -> 1)).asInstanceOf[java.util.Map[java.lang.String, java.lang.Integer]]

  // create n partitions of the stream for topic “test”, to allow n threads to consume
  val topicMessageStreams = consumer.createMessageStreams(mapForStreams)

  val streams = topicMessageStreams.get(topic)

  // launch all the threads
  val executor = Executors.newFixedThreadPool(numThreads)

  // consume the messages in the threads
  for(stream <- streams) {
    executor.submit(new Runnable() {
      def run() {
        for(msgAndMetadata <- stream) {
          println(s"Consumer received: ${new String(msgAndMetadata.message)}")
          latch.countDown()
        }
      }
    })
  }

  def createConsumerConfig: ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

  def shutdown() {
    println("Consumer shutting down.")
    if (consumer != null) consumer.shutdown()
    if (executor != null) executor.shutdown()
  }
}