package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkConf

import com.datastax.driver.core.ConsistencyLevel

/** Read settings for RDD
  *
  * @param splitSize number of Cassandra partitions to be read in a single Spark task
  * @param fetchSize number of CQL rows to fetch in a single round-trip to Cassandra
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality */
case class ReadConf(
  splitSize: Int = ReadConf.DefaultSplitSize,
  fetchSize: Int = ReadConf.DefaultFetchSize,
  consistencyLevel: ConsistencyLevel = ReadConf.DefaultConsistencyLevel)


object ReadConf {
  val DefaultSplitSize = 100000
  val DefaultFetchSize = 1000
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE

  def fromSparkConf(conf: SparkConf, cluster: Option[String] = None): ReadConf = {
    ReadConf(
      fetchSize = conf.getInt(CassandraConnectorConf.processProperty("spark.cassandra.input.page.row.size", cluster), DefaultFetchSize),
      splitSize = conf.getInt(CassandraConnectorConf.processProperty("spark.cassandra.input.split.size", cluster), DefaultSplitSize),
      consistencyLevel = ConsistencyLevel.valueOf(
        conf.get(CassandraConnectorConf.processProperty("spark.cassandra.input.consistency.level", cluster), DefaultConsistencyLevel.name()))
    )
  }
}

