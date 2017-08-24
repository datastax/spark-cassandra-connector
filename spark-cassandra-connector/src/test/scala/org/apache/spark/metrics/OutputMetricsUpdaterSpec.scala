package org.apache.spark.metrics

import java.util.concurrent.CountDownLatch

import org.apache.spark.executor.{DataWriteMethod, OutputMetrics, TaskMetrics}
import org.apache.spark.metrics.source.Source
import org.apache.spark.{SparkConf, TaskContext}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import com.datastax.spark.connector.writer.{RichStatement, WriteConf}

class OutputMetricsUpdaterSpec extends FlatSpec with Matchers with BeforeAndAfter with MockitoSugar {

  val ts = System.currentTimeMillis()

  private def newTaskContext(useTaskMetrics: Boolean = true)(sources: Source*): TaskContext = {
    val tc = mock[TaskContext]
    if (useTaskMetrics) {
      when(tc.taskMetrics()) thenReturn new TaskMetrics
    }
    when(tc.getMetricsSources(MetricsUpdater.cassandraConnectorSourceName)) thenReturn sources
    tc
  }

  private def newRichStatement(): RichStatement = {
    new RichStatement() {
      override val bytesCount = 100
      override val rowsCount = 10
    }
  }

  it should "create updater which uses task metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.output.metrics", "true")
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    tc.taskMetrics().outputMetrics.bytesWritten shouldBe 100L // change registered when success
    tc.taskMetrics().outputMetrics.recordsWritten shouldBe 10L

    updater.batchFinished(success = false, rc, ts, ts)
    tc.taskMetrics().outputMetrics.bytesWritten shouldBe 100L // change not regsitered when failure
    tc.taskMetrics().outputMetrics.recordsWritten shouldBe 10L
  }

  it should "create updater which does not use task metrics" in {
    val tc = newTaskContext(useTaskMetrics = false)()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.output.metrics", "false")
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    updater.batchFinished(success = false, rc, ts, ts)

    verify(tc, never).taskMetrics()
  }

  it should "create updater which uses Codahale metrics" in {
    val ccs = new CassandraConnectorSource
    val tc = newTaskContext()(ccs)
    val conf = new SparkConf(loadDefaults = false)
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    ccs.writeRowMeter.getCount shouldBe 10L
    ccs.writeByteMeter.getCount shouldBe 100L
    ccs.writeSuccessCounter.getCount shouldBe 1L
    ccs.writeFailureCounter.getCount shouldBe 0L
    ccs.writeBatchSizeHistogram.getSnapshot.getMedian shouldBe 10.0
    ccs.writeBatchSizeHistogram.getCount shouldBe 1L

    updater.batchFinished(success = false, rc, ts, ts)
    ccs.writeRowMeter.getCount shouldBe 10L
    ccs.writeByteMeter.getCount shouldBe 100L
    ccs.writeSuccessCounter.getCount shouldBe 1L
    ccs.writeFailureCounter.getCount shouldBe 1L
    ccs.writeBatchSizeHistogram.getCount shouldBe 1L

    updater.finish()
    ccs.writeTaskTimer.getCount shouldBe 1L
  }

  it should "create updater which doesn't use Codahale metrics" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    updater.batchFinished(success = true, rc, ts, ts)
    updater.batchFinished(success = false, rc, ts, ts)

    updater.finish()
  }

  it should "work correctly with multiple threads" in {
    val tc = newTaskContext()()
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.cassandra.output.metrics", "true")
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = newRichStatement()

    val latch = new CountDownLatch(32)
    class TestThread extends Thread {
      override def run(): Unit = {
        latch.countDown()
        latch.await()
        for (i <- 1 to 100000)
          updater.batchFinished(success = true, rc, ts, ts)
      }
    }

    val threads = Array.fill(32)(new TestThread)
    threads.foreach(_.start())
    threads.foreach(_.join())

    tc.taskMetrics().outputMetrics.bytesWritten shouldBe 320000000L
    tc.taskMetrics().outputMetrics.recordsWritten shouldBe 32000000L
  }

}
