package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.ConfigCheck
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
                     throughputMiBPS: Int = WriteConf.DefaultThroughputMiBPS,
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

  val WriteBatchSizeInBytesProperty = "spark.cassandra.output.batch.size.bytes"
  val WriteConsistencyLevelProperty = "spark.cassandra.output.consistency.level"
  val WriteBatchSizeInRowsProperty = "spark.cassandra.output.batch.size.rows"
  val WriteBatchBufferSizeProperty = "spark.cassandra.output.batch.grouping.buffer.size"
  val WriteBatchLevelProperty = "spark.cassandra.output.batch.grouping.key"
  val WriteParallelismLevelProperty = "spark.cassandra.output.concurrent.writes"
  val WriteThroughputMiBPS = "spark.cassandra.output.throughput_mb_per_sec"
  val WriteTaskMetricsProperty = "spark.cassandra.output.metrics"

  // Whitelist for allowed Write environment variables
  val Properties = Set(
    WriteBatchSizeInBytesProperty,
    WriteConsistencyLevelProperty,
    WriteBatchSizeInRowsProperty,
    WriteBatchBufferSizeProperty,
    WriteBatchLevelProperty,
    WriteParallelismLevelProperty,
    WriteThroughputMiBPS,
    WriteTaskMetricsProperty
  )

  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultBatchSizeInBytes = 1024
  val DefaultParallelismLevel = 5
  val DefaultBatchGroupingBufferSize = 1000
  val DefaultBatchGroupingKey = BatchGroupingKey.Partition
  val DefaultThroughputMiBPS = Int.MaxValue
  val DefaultWriteTaskMetricsEnabled = true

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

    val throughputMiBPS = conf.getInt(
      WriteThroughputMiBPS, DefaultThroughputMiBPS)

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