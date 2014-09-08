package com.datastax.spark.connector.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.server.KafkaServer
import com.datastax.spark.connector.util.Assertions

import scala.concurrent.duration.Duration

trait KafkaStreamingWithCassandra extends Assertions {

  //SparkEnv.get.actorSystem.registerOnTermination(kafka.shutdown())

  def awaitPropagation(servers: Seq[KafkaServer], topic: String, partition: Int, timeout: Duration): Unit = {
    assert(awaitCond(servers.forall(_.apis.leaderCache.
      keySet.contains(TopicAndPartition(topic, partition))), timeout),
      s"Partition [$topic, $partition] metadata not propagated after timeout")
  }
}
