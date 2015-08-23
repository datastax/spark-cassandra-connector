package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.{ConfigParameter, ConfigCheck}
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf

/** Write settings for RDD
  *
  * @param batchSize approx. number of bytes to be written in a single batch or
  *                  exact number of rows to be written in a single batch;
  * @param batchGroupingBufferSize the number of distinct batches that can be buffered before
  *                        they are written to Cassandra
  * @param batchGroupingKey which rows can be grouped into a single batch
  * @param consistencyLevel consistency level for writes, default LOCAL_ONE
  * @param parallelismLevel number of batches to be written in parallel
  * @param ttl       the default TTL value which is used when it is defined (in seconds)
  * @param timestamp the default timestamp value which is used when it is defined (in microseconds)
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+)
  */

case class WriteConf(batchSize: BatchSize = BatchSize.Automatic,
                     batchGroupingBufferSize: Int = WriteConf.DefaultBatchGroupingBufferSize,
                     batchGroupingKey: BatchGroupingKey = WriteConf.DefaultBatchGroupingKey,
                     consistencyLevel: ConsistencyLevel = WriteConf.DefaultConsistencyLevel,
                     parallelismLevel: Int = WriteConf.DefaultParallelismLevel,
                     throughputMiBPS: Double = WriteConf.DefaultThroughputMiBPS,
                     ttl: TTLOption = TTLOption.defaultValue,
                     timestamp: TimestampOption = TimestampOption.defaultValue,
                     taskMetricsEnabled: Boolean = WriteConf.DefaultWriteTaskMetricsEnabled) {

  private[writer] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
    case WriteOption(PerRowWriteOptionValue(placeholder)) => placeholder
  }

  private[writer] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case WriteOption(PerRowWriteOptionValue(placeholder)) =>
        Some(ColumnDef(placeholder, RegularColumn, ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(toRegularColDef(ttl, DataType.cint()), toRegularColDef(timestamp, DataType.bigint())).flatten
  }

  val throttlingEnabled = throughputMiBPS < WriteConf.DefaultThroughputMiBPS
}


object WriteConf {

  val ReferenceSection = "Write Tuning Parameters"

  /** Batch Size Bytes **/
  val WriteBatchSizeInBytesProperty = "spark.cassandra.output.batch.size.bytes"
  val DefaultBatchSizeInBytes = 1024
  val WriteBatchSizeInBytesDescription =
    s"""Maximum total size of the batch in bytes. Overridden by
      |$WriteBatchSizeInRowsProperty
    """.stripMargin
  val BatchSizeBytesParam = ConfigParameter(
    WriteBatchSizeInBytesProperty,
    ReferenceSection,
    Some(DefaultBatchSizeInBytes),
    WriteBatchSizeInBytesDescription)

  /** Write Consistency Level **/
  val WriteConsistencyLevelProperty = "spark.cassandra.output.consistency.level"
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val WriteConsistencyLevelDescription = """Consistency level for writing"""
  val ConsistencyLevelParam = ConfigParameter(
    WriteConsistencyLevelProperty,
    ReferenceSection,
    Some(DefaultConsistencyLevel),
    WriteConsistencyLevelDescription)

  /** Batch Size Rows **/
  val WriteBatchSizeInRowsProperty = "spark.cassandra.output.batch.size.rows"
  val WriteBatchSizeInRowsDescription =
    """Number of rows per single batch. The default is 'auto'
      |which means the connector will adjust the number
      |of rows based on the amount of data
      |in each row""".stripMargin
  val BatchSizeRowsParam = ConfigParameter(
    WriteBatchSizeInRowsProperty,
    ReferenceSection,
    None,
    WriteBatchSizeInRowsDescription)

  /** Batch Grouping Buffer Size **/
  val WriteBatchBufferSizeProperty = "spark.cassandra.output.batch.grouping.buffer.size"
  val DefaultBatchGroupingBufferSize = 1000
  val WriteBatchBufferSizeDescription =
    """ How many batches per single Spark task can be stored in
      |memory before sending to Cassandra""".stripMargin
  val BatchBufferSizeParam = ConfigParameter(
    WriteBatchBufferSizeProperty,
    ReferenceSection,
    Some(DefaultBatchGroupingBufferSize),
    WriteBatchBufferSizeDescription
  )


  /** Batch Grouping Key **/
  val WriteBatchLevelProperty = "spark.cassandra.output.batch.grouping.key"
  val DefaultBatchGroupingKey = BatchGroupingKey.Partition
  val WriteBatchLevelDescription = """Determines how insert statements are grouped into batches. Available values are
    |<ul>
    |  <li> <code> none </code> : a batch may contain any statements </li>
    |  <li> <code> replica_set </code> : a batch may contain only statements to be written to the same replica set </li>
    |  <li> <code> partition </code> : a batch may contain only statements for rows sharing the same partition key value </li>
    |</ul>
    |"""
    .stripMargin
  val BatchLevelParam = ConfigParameter (
    WriteBatchLevelProperty,
    ReferenceSection,
    Some(DefaultBatchGroupingKey),
    WriteBatchLevelDescription
  )

  /** Write Paralleism Level **/
  val WriteParallelismLevelProperty = "spark.cassandra.output.concurrent.writes"
  val DefaultParallelismLevel = 5
  val writeParallelismLevelDescription =
    """Maximum number of batches executed in parallel by a
      | single Spark task""".stripMargin
  val ParallelismLevelParam = ConfigParameter (
    WriteParallelismLevelProperty,
    ReferenceSection,
    Some(DefaultParallelismLevel),
    writeParallelismLevelDescription)
  
  /** Write Throughput MBS **/
  val WriteThroughputMiBPSProperty = "spark.cassandra.output.throughput_mb_per_sec"
  val DefaultThroughputMiBPS = Int.MaxValue
  val writeThroughputMiBPSDescription =
    """*(Floating points allowed)* <br> Maximum write throughput allowed
      | per single core in MB/s. <br> Limit this on long (+8 hour) runs to 70% of your max throughput
      | as seen on a smaller job for stability""".stripMargin
  val ThroughputMiBPSParam = ConfigParameter (
    WriteThroughputMiBPSProperty,
    ReferenceSection,
    Some(DefaultThroughputMiBPS),
    writeThroughputMiBPSDescription)

  /** Task Metrics **/
  val WriteTaskMetricsProperty = "spark.cassandra.output.metrics"
  val DefaultWriteTaskMetricsEnabled = true
  val WriteTaskMetricsDescription = """Sets whether to record connector specific metrics on write"""
  val TaskMetricsParam = ConfigParameter(
    WriteTaskMetricsProperty,
    ReferenceSection,
    Some(DefaultBatchGroupingBufferSize),
    WriteTaskMetricsDescription
  )

  // Whitelist for allowed Write environment variables
  val Properties:Set[ConfigParameter] = Set(
    BatchSizeBytesParam,
    ConsistencyLevelParam,
    BatchSizeRowsParam,
    BatchBufferSizeParam,
    BatchLevelParam,
    ParallelismLevelParam,
    ThroughputMiBPSParam,
    TaskMetricsParam
  )



  def fromSparkConf(conf: SparkConf): WriteConf = {

    ConfigCheck.checkConfig(conf)

    val batchSizeInBytes = conf.getInt(
      WriteBatchSizeInBytesProperty, DefaultBatchSizeInBytes)

    val consistencyLevel = ConsistencyLevel.valueOf(
      conf.get(WriteConsistencyLevelProperty, DefaultConsistencyLevel.name()))

    val batchSizeInRowsStr = conf.get(
      WriteBatchSizeInRowsProperty, "auto")

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

    val batchBufferSize = conf.getInt(
      WriteBatchBufferSizeProperty, DefaultBatchGroupingBufferSize)

    val batchGroupingKey = conf.getOption(
      WriteBatchLevelProperty).map(BatchGroupingKey.apply).getOrElse(DefaultBatchGroupingKey)

    val parallelismLevel = conf.getInt(
      WriteParallelismLevelProperty, DefaultParallelismLevel)

    val throughputMiBPS = conf.getDouble(
      WriteThroughputMiBPSProperty, DefaultThroughputMiBPS)

    val metricsEnabled = conf.getBoolean(
      WriteTaskMetricsProperty, DefaultWriteTaskMetricsEnabled)
    
    WriteConf(
      batchSize = batchSize,
      batchGroupingBufferSize = batchBufferSize,
      batchGroupingKey = batchGroupingKey,
      consistencyLevel = consistencyLevel,
      parallelismLevel = parallelismLevel,
      throughputMiBPS = throughputMiBPS,
      taskMetricsEnabled = metricsEnabled)
  }

}