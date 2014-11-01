package com.datastax.spark.connector.demo

import scala.util.Try
import scala.sys.process._
import akka.japi.Util.immutableSeq
import com.typesafe.config.ConfigFactory

final class TwitterSettings {

  protected val config = ConfigFactory.load.getConfig("streaming-app")

  val filters: Set[String] = immutableSeq(config.getStringList("filters")).toSet

  /** Attempts to detect Spark master, falls back System property, falls back to config. */
  val SparkMaster: String = Try("dsetool sparkmaster".!!.trim)
    .getOrElse(sys.props.get("spark.master").getOrElse(config.getString("spark.master")))

  val StreamingBatchInterval = config.getInt("spark.streaming.batch.interval")

  val SparkExecutorMemory = config.getBytes("spark.executor.memory")

  val SparkCoresMax = sys.props.get("spark.cores.max").getOrElse(config.getInt("spark.cores.max"))

  val DeployJars: Array[String] = immutableSeq(config.getStringList("spark.jars"))
    .map(path => s"$path.jar").toArray
  require(DeployJars.nonEmpty)

  /** Attempts to detect Cassandra entry point, else falls back to
    * System property, falls back to config,
    * to produce a comma-separated string of hosts. */
  val CassandraSeedNodes: String = {
    val ipPattern = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}""".r

    Try(ipPattern findFirstIn SparkMaster) getOrElse
      sys.props.get("spark.cassandra.connection.host") getOrElse
        immutableSeq(config.getStringList("spark.cassandra.connection.host")).mkString(",")
  }

  val CassandraKeyspace: String = config.getString("spark.cassandra.keyspace")

  val CassandraTable: String = config.getString("spark.cassandra.table")

}
