package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf

/** Write settings for RDD
  *
  * @param batchSize approx. number of bytes to be written in a single batch or
  *                  exact number of rows to be written in a single batch;
  * @param batchBufferSize the number of distinct batches that can be buffered before
  *                        they are written to Cassandra
  * @param batchLevel which rows can be grouped into a single batch
  * @param consistencyLevel consistency level for writes, default LOCAL_ONE
  * @param parallelismLevel number of batches to be written in parallel
  * @param ttl       the default TTL value which is used when it is defined (in seconds)
  * @param timestamp the default timestamp value which is used when it is defined (in microseconds)
  */

case class WriteConf(batchSize: BatchSize = BatchSize.Automatic,
                     batchBufferSize: Int = WriteConf.DefaultBatchBufferSize,
                     batchLevel: BatchLevel = WriteConf.DefaultBatchLevel,
                     consistencyLevel: ConsistencyLevel = WriteConf.DefaultConsistencyLevel,
                     parallelismLevel: Int = WriteConf.DefaultParallelismLevel,
                     ttl: TTLOption = TTLOption.auto,
                     timestamp: TimestampOption = TimestampOption.auto) {

  private[writer] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
    case PerRowWriteOption(placeholder) => placeholder
  }

  private[writer] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case PerRowWriteOption(placeholder) =>
        Some(ColumnDef(keyspace, table, placeholder, RegularColumn, ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(toRegularColDef(ttl, DataType.cint()), toRegularColDef(timestamp, DataType.bigint())).flatten
  }

}


object WriteConf {
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultBatchSizeInBytes = 16 * 1024
  val DefaultParallelismLevel = 8
  val DefaultBatchBufferSize = 1000
  val DefaultBatchLevel = BatchLevel.Partition

  def fromSparkConf(conf: SparkConf): WriteConf = {

    val batchSizeInBytes = conf.getInt(
      "spark.cassandra.output.batch.size.bytes", DefaultBatchSizeInBytes)

    val consistencyLevel = ConsistencyLevel.valueOf(
      conf.get("spark.cassandra.output.consistency.level", DefaultConsistencyLevel.name()))

    val batchSizeInRowsStr = conf.get(
      "spark.cassandra.output.batch.size.rows", "auto")

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
      "spark.cassandra.output.batch.buffer.size", DefaultBatchBufferSize)

    val batchLevel = conf.getOption(
      "spark.cassandra.output.batch.level").map(BatchLevel.apply).getOrElse(DefaultBatchLevel)

    val parallelismLevel = conf.getInt(
      "spark.cassandra.output.concurrent.writes", DefaultParallelismLevel)

    WriteConf(
      batchSize = batchSize,
      consistencyLevel = consistencyLevel,
      parallelismLevel = parallelismLevel)

  }

}