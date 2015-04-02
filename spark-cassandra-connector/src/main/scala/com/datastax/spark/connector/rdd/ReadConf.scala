package com.datastax.spark.connector.rdd

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.util.ConfigCheck
import org.apache.spark.SparkConf

/** Read settings for RDD
  *
  * @param splitSizeInMB number of Cassandra partitions to be read in a single Spark task
  * @param fetchSizeInRows number of CQL rows to fetch in a single round-trip to Cassandra
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+) */
case class ReadConf(
  splitSizeInMB: Int = ReadConf.DefaultSplitSizeInMB,
  fetchSizeInRows: Int = ReadConf.DefaultFetchSizeInRows,
  consistencyLevel: ConsistencyLevel = ReadConf.DefaultConsistencyLevel,
  taskMetricsEnabled: Boolean = ReadConf.DefaultReadTaskMetricsEnabled)


object ReadConf {

  val ReadFetchSizeInRowsProperty = "spark.cassandra.input.fetch.size_in_rows"
  val ReadSplitSizeInMBProperty = "spark.cassandra.input.split.size_in_mb"
  val ReadConsistencyLevelProperty = "spark.cassandra.input.consistency.level"
  val ReadTaskMetricsProperty = "spark.cassandra.input.metrics"

  // Whitelist for allowed Read environment variables
  val Properties = Set(
    ReadFetchSizeInRowsProperty,
    ReadSplitSizeInMBProperty,
    ReadConsistencyLevelProperty,
    ReadTaskMetricsProperty
  )

  val DefaultSplitSizeInMB = 64 // 64 MB
  val DefaultFetchSizeInRows = 1000
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultReadTaskMetricsEnabled = true

  def fromSparkConf(conf: SparkConf): ReadConf = {

    ConfigCheck.checkConfig(conf)

    ReadConf(
      fetchSizeInRows = conf.getInt(ReadFetchSizeInRowsProperty, DefaultFetchSizeInRows),
      splitSizeInMB = conf.getInt(ReadSplitSizeInMBProperty, DefaultSplitSizeInMB),
      consistencyLevel = ConsistencyLevel.valueOf(
        conf.get(ReadConsistencyLevelProperty, DefaultConsistencyLevel.name())),
      taskMetricsEnabled = conf.getBoolean(ReadTaskMetricsProperty, DefaultReadTaskMetricsEnabled)
    )
  }

}

