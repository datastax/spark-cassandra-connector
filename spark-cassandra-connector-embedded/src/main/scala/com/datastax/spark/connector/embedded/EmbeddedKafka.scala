package com.datastax.spark.connector.embedded

import java.io.File
import java.util.Properties

import scala.concurrent.duration.{Duration, _}
import org.apache.spark.Logging
import kafka.producer._
import kafka.admin.CreateTopicCommand
import kafka.common.TopicAndPartition
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

final class EmbeddedKafka extends Embedded with Logging {

  val kafkaParams = Map(
    "zookeeper.connect" -> ZookeeperConnectionString,
    "group.id" -> s"test-consumer-${scala.util.Random.nextInt(10000)}",
    "auto.offset.reset" -> "smallest")

  private val brokerConf = new Properties()
  brokerConf.put("broker.id", "0")
  brokerConf.put("host.name", "localhost")
  brokerConf.put("port", "9092")
  brokerConf.put("log.dir", createTempDir.getAbsolutePath)
  brokerConf.put("zookeeper.connect", ZookeeperConnectionString)
  brokerConf.put("log.flush.interval.messages", "1")
  brokerConf.put("replica.socket.timeout.ms", "1500")

  /** Starts the ZK server. */
  private val zookeeper = new EmbeddedZookeeper()
  awaitCond(zookeeper.isRunning, 2000.millis)

  log.info(s"Attempting to connect with $ZookeeperConnectionString")
  val client = new ZkClient(ZookeeperConnectionString, 6000, 6000, ZKStringSerializer)
  log.info(s"ZooKeeper Client connected.")

  log.info(s"Attempting to connect KafkaServer with $ZookeeperConnectionString")
  val kafkaConfig = new KafkaConfig(brokerConf)
  val server = new KafkaServer(kafkaConfig)
  Thread.sleep(2000)

  log.info(s"Starting the Kafka server at $ZookeeperConnectionString")
  server.startup()
  Thread.sleep(2000)

  def createTopic(topic: String) {
    CreateTopicCommand.createTopic(client, topic, 1, 1, "0")
    awaitPropagation(Seq(server), topic, 0, 1000.millis)
  }

  def produceAndSendMessage(topic: String, sent: Map[String, Int]) {
    val p = new Properties()
    p.put("metadata.broker.list", kafkaConfig.hostName + ":" + kafkaConfig.port)
    p.put("serializer.class", classOf[StringEncoder].getName)

    val producer = new Producer[String, String](new ProducerConfig(p))
    producer.send(createTestMessage(topic, sent): _*)
    producer.close()
  }

  private def createTestMessage(topic: String, send: Map[String, Int]): Seq[KeyedMessage[String, String]] =
    (for ((s, freq) <- send; i <- 0 until freq) yield new KeyedMessage[String, String](topic, s)).toSeq

  def awaitPropagation(servers: Seq[KafkaServer], topic: String, partition: Int, timeout: Duration): Unit =
    awaitCond(
      p = servers.forall(_.apis.leaderCache.keySet.contains(TopicAndPartition(topic, partition))),
      max = timeout,
      message = s"Partition [$topic, $partition] metadata not propagated after timeout")

  def shutdown(): Unit = {
    log.info(s"Shutting down Kafka server.")
    server.shutdown()
    server.config.logDirs.foreach(f => deleteRecursively(new File(f)))
    log.info(s"Shutting down ZK client.")
    client.close()
    zookeeper.shutdown()
    awaitCond(!zookeeper.isRunning, 2000.millis)
    log.info(s"ZooKeeper server shut down.")
  }
}

