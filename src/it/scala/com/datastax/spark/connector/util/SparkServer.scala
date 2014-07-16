package com.datastax.spark.connector.util

import org.apache.spark.{SparkContext, SparkConf}

trait SparkServer {
  val conf = SparkServer.conf
  val sc = SparkServer.sc
}

object SparkServer {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", CassandraServer.cassandraHost.getHostAddress)
  val sparkMasterUrl = Option(System.getenv("IT_TEST_SPARK_MASTER")).getOrElse("local")
  val sc = new SparkContext(sparkMasterUrl, "Integration Test", conf)
}
