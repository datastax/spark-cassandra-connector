package com.datastax.spark.connector.demo

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector.testkit._

trait DemoApp {

  val sparkMasterHost = DefaultHost
  val cassandraHost = DefaultHost

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)

  // Connect to the Spark cluster:
  val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "demo-program", conf)
}

object DemoApp {
  def apply(): DemoApp = new DemoApp {}
}
