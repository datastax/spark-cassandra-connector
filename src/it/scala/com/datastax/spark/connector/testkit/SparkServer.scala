package com.datastax.spark.connector.testkit

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

trait SparkServer {
  val conf = SparkServer.conf
  val sc = SparkServer.sc
}

object SparkServer {
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraServer.cassandraHost.getHostAddress)
    .set("spark.cleaner.ttl", "3600")   // required for Spark Streaming
  val sparkMasterUrl = Option(System.getenv("IT_TEST_SPARK_MASTER")).getOrElse("local[4]")
  val sc = new SparkContext(sparkMasterUrl, "Integration Test", conf)
  val actorSystem = SparkEnv.get.actorSystem
}
