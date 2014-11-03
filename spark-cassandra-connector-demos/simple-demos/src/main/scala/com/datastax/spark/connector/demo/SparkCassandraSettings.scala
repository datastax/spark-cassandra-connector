package com.datastax.spark.connector.demo

import com.typesafe.config.{Config, ConfigFactory}

/* Initializes Akka, Cassandra and Spark settings. */
final class SparkCassandraSettings(rootConfig: Config) {
  def this() = this(ConfigFactory.load)

  protected val config = rootConfig.getConfig("streaming-demo")

  val SparkMaster: String = config.getString("spark.master")

  val SparkCleanerTtl: Int = config.getInt("spark.cleaner.ttl")

  val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")

  val Data = akka.japi.Util.immutableSeq(config.getStringList("data")).toSet

  val CassandraSeed: String = config.getString("spark.cassandra.connection.host")

  val CassandraKeyspace = config.getString("spark.cassandra.keyspace")

  val CassandraTable = config.getString("spark.cassandra.table")
}