package com.datastax.spark.connector.demo

import org.apache.spark.{SparkContext, SparkConf}

trait DemoApp {

  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "127.0.0.1"

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true).set("cassandra.connection.host", cassandraHost)

  // Connect to the Spark cluster:
  val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "demo-program", conf)
}
