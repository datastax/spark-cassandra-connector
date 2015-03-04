package com.datastax.spark.connector.demo

import org.apache.spark.Logging

object KafkaStreamingScala211App extends App with Logging {

  log.info("Spark is not yet supporting Kafka with Scala 2.11, or publishing the spark-streaming-kafka artifact.You can run the demo against Scala 2.10 only so far.")
}