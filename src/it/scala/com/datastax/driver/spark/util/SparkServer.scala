package com.datastax.driver.spark.util

import com.datastax.driver.spark.connector.CassandraConnector
import org.apache.spark.{SparkContext, SparkConf}

trait SparkServer {
  val conf = SparkServer.conf
  val sc = SparkServer.sc
}

object SparkServer {
  val conf = new SparkConf(true).set("cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local", "Integration Test", conf)
}
