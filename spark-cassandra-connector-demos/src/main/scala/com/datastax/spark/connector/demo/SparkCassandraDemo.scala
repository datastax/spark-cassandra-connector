package com.datastax.spark.connector.demo

import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.util.Logging
import org.apache.spark.{SparkContext, SparkConf}

/** Checks for cassandra host and spark master passed in with -D
  * 'spark.master' and 'spark.cassandra.connection.host'.
  * Falls back to defaults if not found.
  */
trait SparkCassandraDemo extends App with Logging {

  val sparkMaster = sys.props.get("spark.master")
    .getOrElse("local[12]")

  val cassandraHost = sys.props.get("spark.cassandra.connection.host")
    .getOrElse(EmbeddedCassandra.cassandraHost.getHostName)


  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", cassandraHost)
    .set("spark.cleaner.ttl", "3600")
    .setMaster(sparkMaster)
    .setAppName(getClass.getSimpleName)

  /** Connect to the Spark cluster: */
  lazy val sc = new SparkContext(conf)
}

object SparkCassandraDemo {
  def apply(): SparkCassandraDemo = new SparkCassandraDemo { }
}
