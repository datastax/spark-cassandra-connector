package com.datastax.spark.connector.embedded

import java.io.File
import java.util.Properties

import scala.concurrent.duration.{Duration, _}
import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

final class EmbeddedKafka(val kafkaParams: Map[String,String]) extends Embedded {

  def this(groupId: String) = this(Map(
    "zookeeper.connect" -> ZookeeperConnectionString,
    "group.id" -> groupId,
    "auto.offset.reset" -> "smallest"))

  def this() = this(s"consumer-${scala.util.Random.nextInt(10000)}")

  /** Starts the ZK server. */
  private val zookeeper = new EmbeddedZookeeper()
  awaitCond(zookeeper.isRunning, 2000.millis)

  val client = new ZkClient(ZookeeperConnectionString, 6000, 6000, ZKStringSerializer)
  println(s"ZooKeeper Client connected.")

  val kafkaConfig: KafkaConfig = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", "9092")
    props.put("log.dir", createTempDir.getAbsolutePath)
    props.put("zookeeper.connect", ZookeeperConnectionString)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
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
    AdminUtils.createTopic(client, topic, numPartitions, replicationFactor)
    awaitPropagation(Seq(server), topic, 0, 2000.millis)
  }

  def produceAndSendMessage(topic: String, sent: Map[String, Int]): Unit = {
    producer.send(createTestMessage(topic, sent): _*)
  }

  private def createTestMessage(topic: String, send: Map[String, Int]): Seq[KeyedMessage[String, String]] =
    (for ((s, freq) <- send; i <- 0 until freq) yield new KeyedMessage[String, String](topic, s)).toSeq

  def awaitPropagation(servers: Seq[KafkaServer], topic: String, partition: Int, timeout: Duration): Unit =
    awaitCond(
      servers.forall { server =>
         val partitionStateOpt = server.apis.metadataCache.getPartitionInfo(topic, partition)
         partitionStateOpt match {
           case Some(partitionState) =>
             partitionState.leaderIsrAndControllerEpoch.leaderAndIsr.leader >= 0 // is valid broker id
           case _ => false
         }
       },
      max = timeout,
      message = s"Partition [$topic, $partition] metadata not propagated after timeout"
    )

  def shutdown(): Unit = try {
    println(s"Shutting down Kafka server.")
    client.close()

    server.shutdown()
    server.config.logDirs.foreach(f => deleteRecursively(new File(f)))

    zookeeper.shutdown()
    awaitCond(!zookeeper.isRunning, 2000.millis)
    println(s"ZooKeeper server shut down.")
    Thread.sleep(2000)
  } catch { case e: java.io.IOException => }
}

