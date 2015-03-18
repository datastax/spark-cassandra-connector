package com.datastax.spark.connector.rdd

import com.datastax.driver.core.ConsistencyLevel
import org.apache.spark.SparkConf

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

  val ReadFetchSizeProperty = "spark.cassandra.input.page.row.size"
  val ReadSplitSizeProperty = "spark.cassandra.input.split.size"
  val ReadConsistencyLevelProperty = "spark.cassandra.input.consistency.level"

  //Whitelist for allowed Read environment variables
  val Properties = Seq(
    ReadFetchSizeProperty,
    ReadSplitSizeProperty,
    ReadConsistencyLevelProperty
  )

  val DefaultSplitSize = 100000
  val DefaultFetchSize = 1000
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE

  def fromSparkConf(conf: SparkConf): ReadConf = {
    ReadConf(
      fetchSize = conf.getInt(ReadFetchSizeProperty, DefaultFetchSize),
      splitSize = conf.getInt(ReadSplitSizeProperty, DefaultSplitSize),
      consistencyLevel = ConsistencyLevel.valueOf(
        conf.get(ReadConsistencyLevelProperty, DefaultConsistencyLevel.name()))
    )
  }

}

