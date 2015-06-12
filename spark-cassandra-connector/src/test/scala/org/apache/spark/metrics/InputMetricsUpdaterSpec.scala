package org.apache.spark.metrics

import org.apache.spark.executor.{DataReadMethod, TaskMetrics}
import org.apache.spark.{TaskContext, SparkConf, SparkEnv}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar

import com.datastax.driver.core.RowMock
import com.datastax.spark.connector.rdd.ReadConf

class InputMetricsUpdaterSpec extends FlatSpec with Matchers with BeforeAndAfter with MockitoSugar {

  after {
    SparkEnv.set(null)
  }

  private def newTaskContext(): TaskContext = {
    val tc = mock[TaskContext]
    when(tc.taskMetrics()) thenReturn new TaskMetrics
    tc
  }

  "InputMetricsUpdater" should "initialize task metrics properly when they are empty" in {
    val tc = newTaskContext()
    tc.taskMetrics().setInputMetrics(None)

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.input.metrics", "true")

    SparkEnv.set(mock[SparkEnv])
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    tc.taskMetrics().inputMetrics.isDefined shouldBe true
    tc.taskMetrics().inputMetrics.get.readMethod shouldBe DataReadMethod.Hadoop
    tc.taskMetrics().inputMetrics.get.bytesRead shouldBe 0L
    tc.taskMetrics().inputMetrics.get.recordsRead shouldBe 0L
  }

  it should "create updater which uses task metrics" in {
    val tc = newTaskContext()
    tc.taskMetrics().setInputMetrics(None)

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.input.metrics", "true")

    SparkEnv.set(mock[SparkEnv])
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))
    updater.updateMetrics(row)
    tc.taskMetrics().inputMetrics.get.bytesRead shouldBe 10L
    tc.taskMetrics().inputMetrics.get.recordsRead shouldBe 1L

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    tc.taskMetrics().inputMetrics.get.bytesRead shouldBe 40L
    tc.taskMetrics().inputMetrics.get.recordsRead shouldBe 4L
  }

  it should "create updater which does not use task metrics" in {
    val tc = mock[TaskContext]

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.input.metrics", "false")

    SparkEnv.set(mock[SparkEnv])
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)

    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))
    updater.updateMetrics(row)

    verify(tc, never).taskMetrics()
  }

  it should "create updater which uses Codahale metrics" in {
    val tc = newTaskContext()

    val conf = new SparkConf(loadDefaults = false)
    SparkEnv.set(mock[SparkEnv])
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
    val tc = newTaskContext()
    val conf = new SparkConf(loadDefaults = false)
    SparkEnv.set(mock[SparkEnv])

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
