package com.datastax.spark.connector.demo

import org.apache.spark.{Logging, SparkContext, SparkConf}

trait DemoApp extends App with Logging {

  val words = "./spark-cassandra-connector-demos/simple-demos/src/main/resources/data/words"

  val SparkMasterHost = "127.0.0.1"

  val CassandraHost = "127.0.0.1"

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraHost)
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[12]")
    .setAppName(getClass.getSimpleName)

  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)
}

object DemoApp {
  def apply(): DemoApp = new DemoApp {}
}
