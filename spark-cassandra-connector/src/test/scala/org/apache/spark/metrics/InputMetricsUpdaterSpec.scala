package org.apache.spark.metrics

import org.apache.spark.executor.{DataReadMethod, TaskMetrics}
import org.apache.spark.metrics.source.Source
import org.apache.spark.{SparkConf, TaskContext}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import com.datastax.driver.core.RowMock
import com.datastax.spark.connector.rdd.ReadConf

class InputMetricsUpdaterSpec extends FlatSpec with Matchers with BeforeAndAfter with MockitoSugar {

  private def newTaskContext(useTaskMetrics: Boolean = true)(sources: Source*): TaskContext = {
    val tc = mock[TaskContext]
    if (useTaskMetrics) {
      when(tc.taskMetrics()) thenReturn new TaskMetrics
    }
    when(tc.getMetricsSources(MetricsUpdater.cassandraConnectorSourceName)) thenReturn sources
    tc
  }

  it should "create updater which uses task metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.input.metrics", "true")
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)
    tc.taskMetrics().inputMetrics.bytesRead shouldBe 10L
    tc.taskMetrics().inputMetrics.recordsRead shouldBe 1L

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    tc.taskMetrics().inputMetrics.bytesRead shouldBe 40L
    tc.taskMetrics().inputMetrics.recordsRead shouldBe 4L
  }

  it should "create updater which does not use task metrics" in {
    val tc = newTaskContext(useTaskMetrics = false)()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.input.metrics", "false")
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)

    verify(tc, never).taskMetrics()
  }

  it should "create updater which uses Codahale metrics" in {
    val ccs = new CassandraConnectorSource
    val tc = newTaskContext()(ccs)
    val conf = new SparkConf(loadDefaults = false)
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
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
    val updater = InputMetricsUpdater(tc, ReadConf.fromSparkConf(conf), 3)
    val row = new RowMock(Some(1), Some(2), Some(3), None, Some(4))

    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)
    updater.updateMetrics(row)

    updater.finish()
  }

}
