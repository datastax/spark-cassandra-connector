package org.apache.spark.metrics

import java.util.concurrent.CountDownLatch

import org.apache.spark.executor.{DataWriteMethod, OutputMetrics, TaskMetrics}
import org.apache.spark.{SparkConf, SparkEnv}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import com.datastax.spark.connector.metrics.{RichStatementMock, SparkEnvMock, TaskContextMock}
import com.datastax.spark.connector.writer.WriteConf

class OutputMetricsUpdaterSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val ts = System.currentTimeMillis()

  after {
    SparkEnv.set(null)
  }

  "OutputMetricsUpdater" should "initialize task metrics properly when they are empty" in {
    val tc = new TaskContextMock
    tc.metrics.outputMetrics = None

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.output.metrics", "true")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))

    tc.metrics.outputMetrics.isDefined shouldBe true
    tc.metrics.outputMetrics.get.writeMethod shouldBe DataWriteMethod.Hadoop
    tc.metrics.outputMetrics.get.bytesWritten shouldBe 0L
    tc.metrics.outputMetrics.get.recordsWritten shouldBe 0L
  }

  it should "initialize task metrics properly when they are defined" in {
    val tc = new TaskContextMock
    tc.metrics.outputMetrics = Some(new OutputMetrics(DataWriteMethod.Hadoop))

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.output.metrics", "true")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))

    tc.metrics.outputMetrics.isDefined shouldBe true
    tc.metrics.outputMetrics.get.writeMethod shouldBe DataWriteMethod.Hadoop
    tc.metrics.outputMetrics.get.bytesWritten shouldBe 0L
    tc.metrics.outputMetrics.get.recordsWritten shouldBe 0L
  }

  it should "create updater which uses task metrics" in {
    val tc = new TaskContextMock
    tc.metrics.outputMetrics = None

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.output.metrics", "true")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))

    val rc = new RichStatementMock(100, 10)
    updater.batchFinished(success = true, rc, ts, ts)
    tc.metrics.outputMetrics.get.bytesWritten shouldBe 100L // change registered when success
    tc.metrics.outputMetrics.get.recordsWritten shouldBe 10L

    updater.batchFinished(success = false, rc, ts, ts)
    tc.metrics.outputMetrics.get.bytesWritten shouldBe 100L // change not regsitered when failure
    tc.metrics.outputMetrics.get.recordsWritten shouldBe 10L
  }

  it should "create updater which does not use task metrics" in {
    val tc = new TaskContextMock {
      override def taskMetrics(): TaskMetrics = {
        fail("This should not be called during this test")
      }
    }

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.output.metrics", "false")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))

    val rc = new RichStatementMock(100, 10)
    updater.batchFinished(success = true, rc, ts, ts)
    updater.batchFinished(success = false, rc, ts, ts)

    tc.metrics.outputMetrics shouldBe None
  }

  it should "create updater which uses Codahale metrics" in {
    val tc = new TaskContextMock
    val conf = new SparkConf(loadDefaults = false)
    SparkEnv.set(new SparkEnvMock(conf))
    val ccs = new CassandraConnectorSource
    CassandraConnectorSource.instance should not be None

    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))

    val rc = new RichStatementMock(100, 10)
    updater.batchFinished(success = true, rc, ts, ts)
    ccs.writeRowMeter.getCount shouldBe 10L
    ccs.writeByteMeter.getCount shouldBe 100L
    ccs.writeSuccessCounter.getCount shouldBe 1L
    ccs.writeFailureCounter.getCount shouldBe 0L

    updater.batchFinished(success = false, rc, ts, ts)
    ccs.writeRowMeter.getCount shouldBe 10L
    ccs.writeByteMeter.getCount shouldBe 100L
    ccs.writeSuccessCounter.getCount shouldBe 1L
    ccs.writeFailureCounter.getCount shouldBe 1L

    updater.finish()
    ccs.writeTaskTimer.getCount shouldBe 1L
  }

  it should "create updater which doesn't use Codahale metrics" in {
    val tc = new TaskContextMock
    val conf = new SparkConf(loadDefaults = false)
    SparkEnv.set(new SparkEnvMock(conf))

    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))
    val rc = new RichStatementMock(100, 10)
    updater.batchFinished(success = true, rc, ts, ts)
    updater.batchFinished(success = false, rc, ts, ts)

    CassandraConnectorSource.instance shouldBe None

    updater.finish()
  }

  it should "work correctly with multiple threads" in {
    val tc = new TaskContextMock
    tc.metrics.outputMetrics = None

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.cassandra.output.metrics", "true")

    SparkEnv.set(new SparkEnvMock(conf))
    val updater = OutputMetricsUpdater(tc, WriteConf.fromSparkConf(conf))

    val rc = new RichStatementMock(100, 10)

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

    tc.metrics.outputMetrics.get.bytesWritten shouldBe 320000000L
    tc.metrics.outputMetrics.get.recordsWritten shouldBe 32000000L
  }

}
