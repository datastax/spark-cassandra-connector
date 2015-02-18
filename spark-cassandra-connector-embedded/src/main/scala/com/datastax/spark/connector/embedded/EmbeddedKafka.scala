package com.datastax.spark.connector.embedded

import java.io.File
import java.util.Properties

import scala.util.Try
import scala.concurrent.duration.{Duration, _}
import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}

final class EmbeddedKafka(val kafkaParams: Map[String,String]) extends Embedded {

  def this(groupId: String) = this(Map(
    "zookeeper.connect" -> ZookeeperConnectionString,
    "group.id" -> groupId,
    "auto.offset.reset" -> "smallest"))

  def this() = this(s"consumer-${scala.util.Random.nextInt(10000)}")

  /** Starts the ZK server. */
  private val zookeeper = new EmbeddedZookeeper()
  awaitCond(zookeeper.isRunning, 2000.millis)

  val kafkaConfig: KafkaConfig = {
    import scala.collection.JavaConversions._
    val map = Map(
      "broker.id" -> "0",
      "host.name" -> "127.0.0.1",
      "port" -> "9092",
      "advertised.host.name" -> "127.0.0.1",
      "advertised.port" -> "9092",
      "log.dir" -> createTempDir.getAbsolutePath,
      "zookeeper.connect" -> ZookeeperConnectionString,
      "replica.high.watermark.checkpoint.interval.ms" -> "5000",
      "log.flush.interval.messages" -> "1",
      "replica.socket.timeout.ms" -> "500",
      "controlled.shutdown.enable" -> "false",
      "auto.leader.rebalance.enable" -> "false"
    )
    val props = new Properties()
    props.putAll(map)
    new KafkaConfig(props)
  }

  val server = new KafkaServer(kafkaConfig)
  Thread.sleep(2000)

  println(s"Starting the Kafka server at $ZookeeperConnectionString")
  server.startup()
  Thread.sleep(2000)

  val producerConfig: ProducerConfig = {
    val p = new Properties()
    p.put("metadata.broker.list", kafkaConfig.hostName + ":" + kafkaConfig.port)
    p.put("serializer.class", classOf[StringEncoder].getName)
    new ProducerConfig(p)
  }

  val producer = new Producer[String, String](producerConfig)

  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1) {
    AdminUtils.createTopic(server.zkClient, topic, numPartitions, replicationFactor)
    awaitPropagation(topic, 0, 2000.millis)
  }

  def produceAndSendMessage(topic: String, sent: Map[String, Int]): Unit = {
    producer.send(createTestMessage(topic, sent): _*)
  }

  private def createTestMessage(topic: String, send: Map[String, Int]): Seq[KeyedMessage[String, String]] =
    (for ((s, freq) <- send; i <- 0 until freq) yield new KeyedMessage[String, String](topic, s)).toSeq

  def awaitPropagation(topic: String, partition: Int, timeout: Duration): Unit =
    awaitCond(
      server.apis.metadataCache.getPartitionInfo(topic, partition)
        .exists(_.leaderIsrAndControllerEpoch.leaderAndIsr.leader >= 0),
      max = timeout,
      message = s"Partition [$topic, $partition] metadata not propagated after timeout"
    )

  def shutdown(): Unit = try {
    println(s"Shutting down Kafka server.")
    Option(producer).map(_.close())
    //https://issues.apache.org/jira/browse/KAFKA-1887
    Try(server.kafkaController.shutdown())
    Try(server.shutdown())
    server.awaitShutdown()
    server.config.logDirs.foreach(f => deleteRecursively(new File(f)))
    zookeeper.shutdown()
    awaitCond(!zookeeper.isRunning, 2000.millis)
    println(s"ZooKeeper server shut down.")
    Thread.sleep(2000)
  } catch { case e: java.io.IOException => }
}

