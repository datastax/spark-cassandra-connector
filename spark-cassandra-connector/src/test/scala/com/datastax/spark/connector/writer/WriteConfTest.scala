package com.datastax.spark.connector.writer

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.{RowsInBatch, BytesInBatch}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class WriteConfTest extends FlatSpec with Matchers {

  "WriteConf" should "be configured with proper defaults" in {
    val conf = new SparkConf(false)
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(WriteConf.DefaultBatchSizeInBytes))
    writeConf.consistencyLevel should be(WriteConf.DefaultConsistencyLevel)
    writeConf.parallelismLevel should be(WriteConf.DefaultParallelismLevel)
  }

  it should "allow to set consistency level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.consistency.level", "THREE")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.consistencyLevel should be(ConsistencyLevel.THREE)
  }

  it should "allow to set parallelism level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.concurrent.writes", "17")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.parallelismLevel should be(17)
  }

  it should "allow to set batch size in bytes" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in bytes when rows are set to auto" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
      .set("spark.cassandra.output.batch.size.rows", "auto")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in rows" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.rows", "12345")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(RowsInBatch(12345))
  }

  it should "allow to set batch level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.level", "any")
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.batchLevel should be(BatchLevel.Any)
  }

  it should "allow to set batch buffer size" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.buffer.size", "30000")
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.batchBufferSize should be(30000)
  }


}
