package com.datastax.spark.connector.embedded

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.KafkaConfig

/** Simple producer for an Akka Actor using string encoder and default partitioner. */
abstract class KafkaProducerActor[K, V] extends Actor with ActorLogging {
  import KafkaEvent._

  def producerConfig: ProducerConfig

  private val producer = new KafkaProducer[K, V](producerConfig)

  override def postStop(): Unit = {
    log.info("Shutting down producer.")
    producer.close()
  }

  def receive = {
    case e: KafkaMessageEnvelope[K,V] => producer.send(e)
  }
}

/** Simple producer using string encoder and default partitioner. */
class KafkaProducer[K, V](producerConfig: ProducerConfig) {

  def this(brokers: Set[String], batchSize: Int, producerType: String, serializerFqcn: String) =
    this(KafkaProducer.createConfig(brokers, batchSize, producerType, serializerFqcn))

  def this(config: KafkaConfig) =
    this(KafkaProducer.defaultConfig(config))

  import KafkaEvent._

  private val producer = new Producer[K, V](producerConfig)

  /** Sends the data, partitioned by key to the topic. */
  def send(e: KafkaMessageEnvelope[K,V]): Unit =
    batchSend(e.topic, e.key, e.messages)

  /* Sends a single message. */
  def send(topic : String, key : K, message : V): Unit =
    batchSend(topic, key, Seq(message))

  def batchSend(topic: String, key: K, batch: Seq[V]): Unit = {
    val messages = batch map (msg => new KeyedMessage[K, V](topic, key, msg))
    producer.send(messages.toArray: _*)
  }

  def close(): Unit = producer.close()

}

object KafkaEvent {
  case class KafkaMessageEnvelope[K,V](topic: String, key: K, messages: V*)
}

object KafkaProducer {

  def createConfig(brokers: Set[String], batchSize: Int, producerType: String, serializerFqcn: String): ProducerConfig = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("serializer.class", serializerFqcn)
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("producer.type", producerType)
    props.put("request.required.acks", "1")
    props.put("batch.num.messages", batchSize.toString)
    new ProducerConfig(props)
  }

  def defaultConfig(config: KafkaConfig): ProducerConfig =
    createConfig(Set(s"${config.hostName}:${config.port}"), 100, "async", classOf[StringEncoder].getName)
}