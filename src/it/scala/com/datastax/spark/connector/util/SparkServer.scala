package com.datastax.spark.connector.util

import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

trait SparkServer {
  val conf = SparkServer.conf
  val sc = SparkServer.sc
}

object SparkServer {
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cleaner.ttl", "3600")   // required for Spark Streaming
  val sc = new SparkContext("local[4]", "Integration Test", conf)
  val actorSystem = SparkEnv.get.actorSystem
}
