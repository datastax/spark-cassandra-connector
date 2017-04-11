package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter}
import com.datastax.spark.connector.{BatchSize, BytesInBatch, ColumnRef, RowsInBatch, TTL, WriteTime}
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf

/** Write settings for RDD
  *
  * @param batchSize approx. number of bytes to be written in a single batch or
  *                  exact number of rows to be written in a single batch;
  * @param batchGroupingBufferSize the number of distinct batches that can be buffered before
  *                        they are written to Cassandra
  * @param batchGroupingKey which rows can be grouped into a single batch
  * @param consistencyLevel consistency level for writes, default LOCAL_QUORUM
  * @param ifNotExists inserting a row should happen only if it does not already exist
  * @param parallelismLevel number of batches to be written in parallel
  * @param ttl       the default TTL value which is used when it is defined (in seconds)
  * @param timestamp the default timestamp value which is used when it is defined (in microseconds)
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+)
  */

case class WriteConf(batchSize: BatchSize = BatchSize.Automatic,
                     batchGroupingBufferSize: Int = WriteConf.BatchBufferSizeParam.default,
                     batchGroupingKey: BatchGroupingKey = WriteConf.BatchLevelParam.default,
                     consistencyLevel: ConsistencyLevel = WriteConf.ConsistencyLevelParam.default,
                     ifNotExists: Boolean = WriteConf.IfNotExistsParam.default,
                     ignoreNulls: Boolean = WriteConf.IgnoreNullsParam.default,
                     parallelismLevel: Int = WriteConf.ParallelismLevelParam.default,
                     throughputMiBPS: Double = WriteConf.ThroughputMiBPSParam.default,
                     ttl: TTLOption = TTLOption.defaultValue,
                     timestamp: TimestampOption = TimestampOption.defaultValue,
                     taskMetricsEnabled: Boolean = WriteConf.TaskMetricsParam.default) {

  private[writer] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
    case WriteOption(PerRowWriteOptionValue(placeholder, _)) => placeholder
  }

  private[writer] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case WriteOption(PerRowWriteOptionValue(placeholder, _)) =>
        Some(ColumnDef(placeholder, RegularColumn, ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(toRegularColDef(ttl, DataType.cint()), toRegularColDef(timestamp, DataType.bigint())).flatten
  }

  private[writer] val optionsAsColumnRef: (Option[Int]) => Seq[ColumnRef] = { tableTTL =>
    def toRegularColRef(opt: WriteOption[_]) = opt match {
      case TTLOption(PerRowWriteOptionValue(placeholder, valueIfNull)) =>
        Some(TTL(
          placeholder,
          Some(placeholder),
          valueIfNull orElse tableTTL orElse Some(0)))

      case TimestampOption(PerRowWriteOptionValue(placeholder, valueIfNull)) =>
        Some(WriteTime(placeholder, Some(placeholder), valueIfNull))

      case _ => None
    }

    Seq(toRegularColRef(ttl), toRegularColRef(timestamp)).flatten
  }

  val throttlingEnabled = throughputMiBPS < WriteConf.ThroughputMiBPSParam.default
}


object WriteConf {

  val ReferenceSection = "Write Tuning Parameters"

  val ConsistencyLevelParam = ConfigParameter[ConsistencyLevel](
    name = "spark.cassandra.output.consistency.level",
    section = ReferenceSection,
    default = ConsistencyLevel.LOCAL_QUORUM,
    description = """Consistency level for writing""")

  val BatchSizeRowsParam = ConfigParameter[Option[Int]](
    name = "spark.cassandra.output.batch.size.rows",
    section = ReferenceSection,
    default = None,
    description = """Number of rows per single batch. The default is 'auto'
      |which means the connector will adjust the number
      |of rows based on the amount of data
      |in each row""".stripMargin)

  val BatchSizeBytesParam = ConfigParameter[Int](
    name = "spark.cassandra.output.batch.size.bytes",
    section = ReferenceSection,
    default = 1024,
    description = s"""Maximum total size of the batch in bytes. Overridden by
      |${BatchSizeRowsParam.name}
    """.stripMargin)

  val BatchBufferSizeParam = ConfigParameter[Int](
    name = "spark.cassandra.output.batch.grouping.buffer.size",
    section = ReferenceSection,
    default = 1000,
    description = """ How many batches per single Spark task can be stored in
      |memory before sending to Cassandra""".stripMargin)


  val BatchLevelParam = ConfigParameter[BatchGroupingKey](
    name = "spark.cassandra.output.batch.grouping.key",
    section = ReferenceSection,
    default  = BatchGroupingKey.Partition,
    description = """Determines how insert statements are grouped into batches. Available values are
    |<ul>
    |  <li> <code> none </code> : a batch may contain any statements </li>
    |  <li> <code> replica_set </code> : a batch may contain only statements to be written to the same replica set </li>
    |  <li> <code> partition </code> : a batch may contain only statements for rows sharing the same partition key value </li>
    |</ul>
    |""".stripMargin)

  val IfNotExistsParam = ConfigParameter[Boolean](
    name = "spark.cassandra.output.ifNotExists",
    section = ReferenceSection,
    default = false,
    description =
      """Determines that the INSERT operation is not performed if a row with the same primary
				|key already exists. Using the feature incurs a performance hit.""".stripMargin)

  val IgnoreNullsParam = ConfigParameter[Boolean](
    name = "spark.cassandra.output.ignoreNulls",
    section = ReferenceSection,
    default = false,
    description =
      """ In Cassandra >= 2.2 null values can be left as unset in bound statements. Setting
        |this to true will cause all null values to be left as unset rather than bound. For
        |finer control see the CassandraOption class""".stripMargin)

  val ParallelismLevelParam = ConfigParameter[Int] (
    name = "spark.cassandra.output.concurrent.writes",
    section = ReferenceSection,
    default = 5,
    description = """Maximum number of batches executed in parallel by a
      | single Spark task""".stripMargin)
  
  val ThroughputMiBPSParam = ConfigParameter[Double] (
    name = "spark.cassandra.output.throughput_mb_per_sec",
    section = ReferenceSection,
    default = Int.MaxValue,
    description = """*(Floating points allowed)* <br> Maximum write throughput allowed
      | per single core in MB/s. <br> Limit this on long (+8 hour) runs to 70% of your max throughput
      | as seen on a smaller job for stability""".stripMargin)

  val TTLParam = ConfigParameter[Int] (
    name = "spark.cassandra.output.ttl",
    section = ReferenceSection,
    default = 0,
    description = """Time To Live(TTL) assigned to writes to Cassandra. A value of 0 means no TTL""".stripMargin)

  /** Task Metrics **/
  val TaskMetricsParam = ConfigParameter[Boolean](
    name = "spark.cassandra.output.metrics",
    section = ReferenceSection,
    default = true,
    description = """Sets whether to record connector specific metrics on write"""
  )

  // Whitelist for allowed Write environment variables
  val Properties: Set[ConfigParameter[_]] = Set(
    BatchSizeBytesParam,
    ConsistencyLevelParam,
    BatchSizeRowsParam,
    BatchBufferSizeParam,
    BatchLevelParam,
    IfNotExistsParam,
    IgnoreNullsParam,
    ParallelismLevelParam,
    ThroughputMiBPSParam,
    TTLParam,
    TaskMetricsParam
  )

  def fromSparkConf(conf: SparkConf): WriteConf = {

    ConfigCheck.checkConfig(conf)

    val batchSizeInBytes = conf.getInt(BatchSizeBytesParam.name, BatchSizeBytesParam.default)

    val consistencyLevel = ConsistencyLevel.valueOf(
      conf.get(ConsistencyLevelParam.name, ConsistencyLevelParam.default.name()))

    val batchSizeInRowsStr = conf.get(BatchSizeRowsParam.name, "auto")

    val ifNotExists = conf.getBoolean(IfNotExistsParam.name, IfNotExistsParam.default)

    val ignoreNulls = conf.getBoolean(IgnoreNullsParam.name, IgnoreNullsParam.default)

    val batchSize = {
      val Number = "([0-9]+)".r
      batchSizeInRowsStr match {
        case "auto" => BytesInBatch(batchSizeInBytes)
        case Number(x) => RowsInBatch(x.toInt)
        case other =>
          throw new ConfigurationException(
            s"Invalid value of spark.cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
      }
    }

    val batchBufferSize = conf.getInt(BatchBufferSizeParam.name, BatchBufferSizeParam.default)

    val batchGroupingKey = conf.getOption(BatchLevelParam.name)
      .map(BatchGroupingKey.apply)
      .getOrElse(BatchLevelParam.default)

    val parallelismLevel = conf.getInt(ParallelismLevelParam.name, ParallelismLevelParam.default)

    val throughputMiBPS = conf.getDouble(ThroughputMiBPSParam.name, ThroughputMiBPSParam.default)

    val metricsEnabled = conf.getBoolean(TaskMetricsParam.name, TaskMetricsParam.default)

    val ttlSeconds = conf.getInt(TTLParam.name, TTLParam.default)

    val ttlOption = if(ttlSeconds == TTLParam.default) TTLOption.defaultValue else TTLOption.constant(ttlSeconds)

    WriteConf(
      batchSize = batchSize,
      batchGroupingBufferSize = batchBufferSize,
      batchGroupingKey = batchGroupingKey,
      consistencyLevel = consistencyLevel,
      parallelismLevel = parallelismLevel,
      throughputMiBPS = throughputMiBPS,
      taskMetricsEnabled = metricsEnabled,
      ttl = ttlOption,      
      ignoreNulls = ignoreNulls,
      ifNotExists = ifNotExists)
  }

}
