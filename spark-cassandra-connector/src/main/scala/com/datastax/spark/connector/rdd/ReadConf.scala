package com.datastax.spark.connector.rdd

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.util.{ConfigParameter, ConfigCheck}
import org.apache.spark.SparkConf

/** Read settings for RDD
  *
  * @param splitCount number of partitions to divide the data into; unset by default
  * @param splitSizeInMB size of Cassandra data to be read in a single Spark task; 
  *                      determines the number of partitions, but ignored if `splitCount` is set
  * @param fetchSizeInRows number of CQL rows to fetch in a single round-trip to Cassandra
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+) */
case class ReadConf(
  splitCount: Option[Int] = None,
  splitSizeInMB: Int = ReadConf.DefaultSplitSizeInMB,
  fetchSizeInRows: Int = ReadConf.DefaultFetchSizeInRows,
  consistencyLevel: ConsistencyLevel = ReadConf.DefaultConsistencyLevel,
  taskMetricsEnabled: Boolean = ReadConf.DefaultReadTaskMetricsEnabled)


object ReadConf {
  val ReferenceSection = "Read Tuning Parameters"

  /** Input Split Size in MB **/
  val ReadSplitSizeInMBProperty = "spark.cassandra.input.split.size_in_mb"
  val DefaultSplitSizeInMB = 64 // 64 MB
  val ReadSplitSizeInMBDescription = """Approx amount of data to be fetched into a Spark partition"""
  val SplitSizeInMBParam = ConfigParameter (
    ReadSplitSizeInMBProperty,
    ReferenceSection,
    Some(DefaultSplitSizeInMB),
    ReadSplitSizeInMBDescription)

  /** Read Fetch Size **/
  val ReadFetchSizeInRowsProperty = "spark.cassandra.input.fetch.size_in_rows"
  val DefaultFetchSizeInRows = 1000
  val ReadFetchSizeInRowsDescription = """Number of CQL rows fetched per driver request"""
  val FetchSizeInRowsParam = ConfigParameter(
    ReadFetchSizeInRowsProperty,
    ReferenceSection,
    Some(DefaultFetchSizeInRows),
    ReadFetchSizeInRowsDescription)

  /** Read Consistency Level **/
  val ReadConsistencyLevelProperty = "spark.cassandra.input.consistency.level"
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val ReadConsistencyLevelDescription = """Consistency level to use when reading	"""
  val ConsistencyLevelParam = ConfigParameter(
    ReadConsistencyLevelProperty,
    ReferenceSection,
    Some(DefaultConsistencyLevel),
    ReadConsistencyLevelDescription)

  /** Read Task Metrics **/
  val ReadTaskMetricsProperty = "spark.cassandra.input.metrics"
  val DefaultReadTaskMetricsEnabled = true
  val ReadTaskMetricsDescription ="""Sets whether to record connector specific metrics on write"""
  val TaskMetricParam = ConfigParameter(
    ReadTaskMetricsProperty,
    ReferenceSection,
    Some(DefaultReadTaskMetricsEnabled),
    ReadTaskMetricsDescription
  )

  // Whitelist for allowed Read environment variables
  val Properties = Set(
    SplitSizeInMBParam,
    FetchSizeInRowsParam,
    ConsistencyLevelParam,
    TaskMetricParam
  )

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

