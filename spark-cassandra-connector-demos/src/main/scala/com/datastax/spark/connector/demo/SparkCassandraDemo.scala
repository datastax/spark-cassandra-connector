package com.datastax.spark.connector.demo

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.util.Logging
import org.apache.spark.{SparkContext, SparkConf}

trait SparkCassandraDemo extends App with EmbeddedCassandra with Logging {

  def clearCache(): Unit = CassandraConnector.evictCache()

  startCassandra()

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", cassandraHost.getHostName)
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[12]")
    .setAppName(getClass.getSimpleName)

  /** Connect to the Spark cluster: */
  lazy val sc = new SparkContext(conf)
}

object SparkCassandraDemo {
  def apply(): SparkCassandraDemo = new SparkCassandraDemo {}
}
