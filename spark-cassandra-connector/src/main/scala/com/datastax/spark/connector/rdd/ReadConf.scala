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
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+)
  * @param throughputJoinQueryPerSec maximum read throughput allowed per single core in query/s while
  *                                  joining a RDD with C* table (joinWithCassandraTable operation)*/
case class ReadConf(
  splitCount: Option[Int] = None,
  splitSizeInMB: Int = ReadConf.SplitSizeInMBParam.default,
  fetchSizeInRows: Int = ReadConf.FetchSizeInRowsParam.default,
  consistencyLevel: ConsistencyLevel = ReadConf.ConsistencyLevelParam.default,
  taskMetricsEnabled: Boolean = ReadConf.TaskMetricParam.default,
  throughputJoinQueryPerSec: Long = ReadConf.ThroughputJoinQueryPerSecParam.default
)


object ReadConf {
  val ReferenceSection = "Read Tuning Parameters"

  val SplitSizeInMBParam = ConfigParameter[Int](
    name = "spark.cassandra.input.split.size_in_mb",
    section = ReferenceSection,
    default = 64,
    description =
      """Approx amount of data to be fetched into a Spark partition. Minimum number of resulting Spark
        | partitions is <code>1 + 2 * SparkContext.defaultParallelism</code>
        |""".stripMargin.filter(_ >= ' '))

  val FetchSizeInRowsParam = ConfigParameter[Int](
    name = "spark.cassandra.input.fetch.size_in_rows",
    section = ReferenceSection,
    default = 1000,
    description = """Number of CQL rows fetched per driver request""")

  val ConsistencyLevelParam = ConfigParameter[ConsistencyLevel](
    name = "spark.cassandra.input.consistency.level",
    section = ReferenceSection,
    default = ConsistencyLevel.LOCAL_ONE,
    description = """Consistency level to use when reading	""")

  val TaskMetricParam = ConfigParameter[Boolean](
    name = "spark.cassandra.input.metrics",
    section = ReferenceSection,
    default = true,
    description = """Sets whether to record connector specific metrics on write"""
  )

  val ThroughputJoinQueryPerSecParam = ConfigParameter[Long] (
    name = "spark.cassandra.input.join.throughput_query_per_sec",
    section = ReferenceSection,
    default = Long.MaxValue,
    description =
      "Maximum read throughput allowed per single core in query/s while joining RDD with Cassandra table")

  // Whitelist for allowed Read environment variables
  val Properties = Set(
    SplitSizeInMBParam,
    FetchSizeInRowsParam,
    ConsistencyLevelParam,
    TaskMetricParam,
    ThroughputJoinQueryPerSecParam
  )

  def fromSparkConf(conf: SparkConf): ReadConf = {

    ConfigCheck.checkConfig(conf)

    ReadConf(
      fetchSizeInRows = conf.getInt(FetchSizeInRowsParam.name, FetchSizeInRowsParam.default),
      splitSizeInMB = conf.getInt(SplitSizeInMBParam.name, SplitSizeInMBParam.default),
      consistencyLevel = ConsistencyLevel.valueOf(conf.get(ConsistencyLevelParam.name, ConsistencyLevelParam.default.name)),
      taskMetricsEnabled = conf.getBoolean(TaskMetricParam.name, TaskMetricParam.default),
      throughputJoinQueryPerSec = conf.getLong(ThroughputJoinQueryPerSecParam.name,
        ThroughputJoinQueryPerSecParam.default)
    )
  }

}

