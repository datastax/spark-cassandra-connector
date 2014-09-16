package com.datastax.spark.connector.demo.streaming

import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}

/* Initializes Akka, Cassandra and Spark settings. */
final class SparkCassandraSettings(rootConfig: Config) {
  def this() = this(ConfigFactory.load)

  protected val config = rootConfig.getConfig("spark-cassandra")

  val SparkMaster: String = config.getString("spark.master")

  val SparkCleanerTtl: Int = config.getInt("spark.cleaner.ttl")

  val CassandraSeed: String = config.getString("spark.cassandra.connection.host")

  val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")
}