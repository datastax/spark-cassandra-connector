package com.datastax.spark.connector.embedded

import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor.{ActorLogging, Actor}
import kafka.serializer.StringDecoder
import kafka.consumer.{Consumer, ConsumerConfig}

/** The KafkaConsumer is a very simple consumer of a single Kafka topic.
  * This is a helpful utility for IT tests to insure data is getting published to Kafka
  * for streaming ingestion upstream.
  */
class KafkaConsumer(zookeeper: String, topic: String, groupId: String, partitions: Int, numThreads: Int, count: AtomicInteger) {

  val connector = Consumer.create(createConsumerConfig)

  // create n partitions of the stream for topic “test”, to allow n threads to consume
  val streams = connector
    .createMessageStreams(Map(topic -> partitions), new StringDecoder(), new StringDecoder())
    .get(topic)

  // launch all the threads
  val executor = Executors.newFixedThreadPool(numThreads)

  // consume the messages in the threads
  for(stream <- streams) {
    executor.submit(new Runnable() {
      def run() {
        for(s <- stream) {
          while(s.iterator.hasNext) {
            count.getAndIncrement
          }
        }
      }
    })
  }

  private def createConsumerConfig: ConsumerConfig = {
    val props = new Properties()
    props.put("consumer.timeout.ms", "2000")
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "10")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

  def shutdown() {
    println("Consumer shutting down.")
    Option(connector) map (_.shutdown())
    Option(executor) map (_.shutdown())
  }
}

/** Simple actor with a Kafka consumer to report the latest message count in a Kafka Topic. */
class KafkaTopicLogger(topic: String, group: String, taskInterval: FiniteDuration = 3.seconds)
  extends Actor with ActorLogging {
  import Event._
  import context.dispatcher

  val atomic = new AtomicInteger(0)

  val consumer = new KafkaConsumer(ZookeeperConnectionString, topic, group, 1, 10, atomic)

  var task = context.system.scheduler.schedule(3.seconds, taskInterval) {
    self ! QueryTask
  }
 
  override def postStop(): Unit = {
    task.cancel
    consumer.shutdown()
  }

  def receive: Actor.Receive = {
    case QueryTask =>
      log.info(s"Kafka message count [{}]", atomic.get)
  }
}