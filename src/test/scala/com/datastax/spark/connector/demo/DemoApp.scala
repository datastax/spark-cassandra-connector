package com.datastax.spark.connector.demo

import org.apache.spark.{SparkContext, SparkConf}

trait DemoApp {

  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "127.0.0.1"

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)

  // Connect to the Spark cluster:
  val sc = new SparkContext("local[4]", "demo-program", conf)
}

object DemoApp {
  def apply(): DemoApp = new DemoApp {}

  case class WordCount(word: String, count: Int)
}
