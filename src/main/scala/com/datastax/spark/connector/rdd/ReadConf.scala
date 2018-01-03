package com.datastax.spark.connector.rdd

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter, DeprecatedConfigParameter, Logging}
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
  * @param readsPerSec maximum read throughput allowed per single core in requests/s while
  *                                  joining an RDD with C* table (joinWithCassandraTable operation)
  *                                  also used by enterprise integrations*/
case class ReadConf(
  splitCount: Option[Int] = None,
  splitSizeInMB: Int = ReadConf.SplitSizeInMBParam.default,
  fetchSizeInRows: Int = ReadConf.FetchSizeInRowsParam.default,
  consistencyLevel: ConsistencyLevel = ReadConf.ConsistencyLevelParam.default,
  taskMetricsEnabled: Boolean = ReadConf.TaskMetricParam.default,
  readsPerSec: Int = ReadConf.ReadsPerSecParam.default,
  parallelismLevel: Int = ReadConf.ParallelismLevelParam.default,
  executeAs: Option[String] = None)


object ReadConf extends Logging {
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

  val ReadsPerSecParam = ConfigParameter[Int] (
    name = "spark.cassandra.input.reads_per_sec",
    section = ReferenceSection,
    default = Int.MaxValue,
    description =
      """Sets max requests per core per second for joinWithCassandraTable and some Enterprise integrations"""
  )

  val ThroughputJoinQueryPerSecParam = DeprecatedConfigParameter (
    name = "spark.cassandra.input.join.throughput_query_per_sec",
    replacementParameter = Some(ReadsPerSecParam),
    deprecatedSince = "DSE 5.1.5")


  val ParallelismLevelParam = ConfigParameter[Int] (
    name = "spark.cassandra.concurrent.reads",
    section = ReferenceSection,
    default = 512,
    description =
      """Sets read parallelism for joinWithCassandra tables"""
  )

  def fromSparkConf(conf: SparkConf): ReadConf = {

    ConfigCheck.checkConfig(conf)

    ReadConf(
      fetchSizeInRows = conf.getInt(FetchSizeInRowsParam.name, FetchSizeInRowsParam.default),
      splitSizeInMB = conf.getInt(SplitSizeInMBParam.name, SplitSizeInMBParam.default),
      consistencyLevel = ConsistencyLevel.valueOf(
        conf.get(ConsistencyLevelParam.name, ConsistencyLevelParam.default.name)),
      taskMetricsEnabled = conf.getBoolean(TaskMetricParam.name, TaskMetricParam.default),
      readsPerSec = conf.getInt(ReadsPerSecParam.name, ReadsPerSecParam.default),
      parallelismLevel = conf.getInt(ParallelismLevelParam.name, ParallelismLevelParam.default)
    )
  }

}

