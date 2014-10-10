package com.datastax.spark.connector.writer

import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf

import com.datastax.driver.core.ConsistencyLevel

/** Write settings for RDD
  *
  * @param batchSizeInBytes approx. number of bytes to be written in a single batch
  * @param batchSizeInRows  exact number of rows to be written in a single batch;
  *                         if set, shadows batchSizeInBytes property
  * @param consistencyLevel consistency level for writes, default LOCAL_ONE
  * @param parallelismLevel number of batches to be written in parallel
  */

case class WriteConf(
  batchSizeInBytes: Int = WriteConf.DefaultBatchSizeInBytes,
  batchSizeInRows: Option[Int] = None,
  consistencyLevel: ConsistencyLevel = WriteConf.DefaultConsistencyLevel,
  parallelismLevel: Int = WriteConf.DefaultParallelismLevel)


object WriteConf {
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultBatchSizeInBytes = 16 * 1024
  val DefaultParallelismLevel = 8

  def fromSparkConf(conf: SparkConf): WriteConf = {

    val batchSizeInBytes = conf.getInt(
      "spark.cassandra.output.batch.size.bytes", DefaultBatchSizeInBytes)

    val consistencyLevel = ConsistencyLevel.valueOf(
      conf.get("spark.cassandra.output.consistency.level", DefaultConsistencyLevel.name()))

    val batchSizeInRowsStr = conf.get(
      "spark.cassandra.output.batch.size.rows", "auto")

    val batchSizeInRows = {
      val Number = "([0-9]+)".r
      batchSizeInRowsStr match {
        case "auto" => None
        case Number(x) => Some(x.toInt)
        case other =>
          throw new ConfigurationException(
            s"Invalid value of spark.cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
      }
    }

    val parallelismLevel = conf.getInt(
      "spark.cassandra.output.concurrent.writes", DefaultParallelismLevel)

    WriteConf(
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = batchSizeInRows,
      consistencyLevel = consistencyLevel,
      parallelismLevel = parallelismLevel)

  }

}