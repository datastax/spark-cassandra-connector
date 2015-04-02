package org.apache.spark.metrics

import org.apache.spark.executor.{DataReadMethod, TaskMetrics}
import org.apache.spark.{SparkConf, SparkEnv}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import com.datastax.driver.core.RowMock
import com.datastax.spark.connector.metrics.{SparkEnvMock, TaskContextMock}
import com.datastax.spark.connector.rdd.ReadConf

class InputMetricsUpdaterSpec extends FlatSpec with Matchers with BeforeAndAfter {

  after {
    SparkEnv.set(null)
  }

  "InputMetricsUpdater" should "initialize task metrics properly when they are empty" in {
    val tc = new TaskContextMock
    tc.metrics.setInputMetrics(None)

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.input.metrics", "true")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    tc.metrics.inputMetrics.isDefined shouldBe true
    tc.metrics.inputMetrics.get.readMethod shouldBe DataReadMethod.Hadoop
    tc.metrics.inputMetrics.get.bytesRead shouldBe 0L
    tc.metrics.inputMetrics.get.recordsRead shouldBe 0L
  }

  it should "create updater which uses task metrics" in {
    val tc = new TaskContextMock
    tc.metrics.setInputMetrics(None)

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.input.metrics", "true")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))
    updater.updateMetrics(row)
    tc.metrics.inputMetrics.get.bytesRead shouldBe 10L
    tc.metrics.inputMetrics.get.recordsRead shouldBe 1L

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    tc.metrics.inputMetrics.get.bytesRead shouldBe 40L
    tc.metrics.inputMetrics.get.recordsRead shouldBe 4L
  }

  it should "create updater which does not use task metrics" in {
    val tc = new TaskContextMock {
      override def taskMetrics(): TaskMetrics = {
        fail("This should not be called during this test")
      }
    }

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.input.metrics", "false")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))
    updater.updateMetrics(row)

    tc.metrics.inputMetrics shouldBe None
  }

  it should "create updater which uses Codahale metrics" in {
    val tc = new TaskContextMock
    val conf = new SparkConf(loadDefaults = false)
    SparkEnv.set(new SparkEnvMock(conf))
    val ccs = new CassandraConnectorSource
    CassandraConnectorSource.instance should not be None

    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))
    updater.updateMetrics(row)
    ccs.readRowMeter.getCount shouldBe 0
    ccs.readByteMeter.getCount shouldBe 0

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)

    ccs.readRowMeter.getCount shouldBe 3L
    ccs.readByteMeter.getCount shouldBe 30L

    updater.finish()
    ccs.readTaskTimer.getCount shouldBe 1L
  }

  it should "create updater which doesn't use Codahale metrics" in {
    val tc = new TaskContextMock
    val conf = new SparkConf(loadDefaults = false)
    SparkEnv.set(new SparkEnvMock(conf))

    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    CassandraConnectorSource.instance shouldBe None

    updater.finish()
  }

}
