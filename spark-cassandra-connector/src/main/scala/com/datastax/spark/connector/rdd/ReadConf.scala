package com.datastax.spark.connector.rdd

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.util.{ConfigParameter, ConfigCheck, Logging}
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
  parallelismLevel: Int = ReadConf.ParallelismLevelParam.default,
  readsPerSec: Int = ReadConf.ReadsPerSecParam.default
)


object ReadConf extends Logging {
  val ReferenceSection = "Read Tuning Parameters"

  val SplitCountParam = ConfigParameter[Option[Int]](
    name = "splitCount",
    section = ReferenceSection,
    default = None,
    description =
      """Specify the number of Spark partitions to
        |read the Cassandra table into. This parameter is
        |used in SparkSql and DataFrame Options.
      """.stripMargin
  )

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
    default = Int.MaxValue,
    description =
      "**Deprecated** Please use input.reads_per_sec. Maximum read throughput allowed per single core in query/s while joining RDD with Cassandra table")

  val ParallelismLevelParam = ConfigParameter[Int] (
    name = "spark.cassandra.concurrent.reads",
    section = ReferenceSection,
    default = 512,
    description =
       """Sets read parallelism for joinWithCassandra tables"""
  )


  val ReadsPerSecParam = ConfigParameter[Int] (
    name = "spark.cassandra.input.reads_per_sec",
    section = ReferenceSection,
    default = Int.MaxValue,
    description =
      """Sets max requests per core per second for joinWithCassandraTable and some Enterprise integrations"""
  )

  // Whitelist for allowed Read environment variables
  val Properties = Set(
    SplitCountParam,
    ConsistencyLevelParam,
    FetchSizeInRowsParam,
    ReadsPerSecParam,
    SplitSizeInMBParam,
    TaskMetricParam,
    ThroughputJoinQueryPerSecParam,
    ParallelismLevelParam
  )

  def fromSparkConf(conf: SparkConf): ReadConf = {

    ConfigCheck.checkConfig(conf)

    val throughtputJoinQueryPerSec = conf.getOption(ThroughputJoinQueryPerSecParam.name)
      .map { str =>
        logWarning(
          s"""${ThroughputJoinQueryPerSecParam.name} is deprecated
             | please use ${ReadsPerSecParam.name}""".stripMargin)
        val longStr = str.toLong
        if (longStr > Int.MaxValue) {
          logDebug(
            s"""${ThroughputJoinQueryPerSecParam.name} was set to a value larger than
               | ${Int.MaxValue} using ${Int.MaxValue}""".stripMargin)
          Int.MaxValue
        } else {
          longStr.toInt
        }
      }

    ReadConf(
      fetchSizeInRows = conf.getInt(FetchSizeInRowsParam.name, FetchSizeInRowsParam.default),
      splitSizeInMB = conf.getInt(SplitSizeInMBParam.name, SplitSizeInMBParam.default),

      consistencyLevel = ConsistencyLevel.valueOf(
        conf.get(ConsistencyLevelParam.name, ConsistencyLevelParam.default.name)),

      taskMetricsEnabled = conf.getBoolean(TaskMetricParam.name, TaskMetricParam.default),
      readsPerSec = conf.getInt(ReadsPerSecParam.name,
        throughtputJoinQueryPerSec.getOrElse(ReadsPerSecParam.default)),
      parallelismLevel = conf.getInt(ParallelismLevelParam.name, ParallelismLevelParam.default),
      splitCount = conf.getOption(SplitCountParam.name).map(_.toInt)
    )
  }

}

