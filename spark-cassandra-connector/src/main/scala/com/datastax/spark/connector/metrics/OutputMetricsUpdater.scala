package com.datastax.spark.connector.metrics

import java.util.concurrent.{Semaphore, TimeUnit}

import com.datastax.spark.connector.writer.{WriteConf, RichStatement}
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.metrics.CassandraConnectorSource
import org.apache.spark.{Logging, SparkEnv, TaskContext}

private[connector] trait OutputMetricsUpdater extends MetricsUpdater {
  def batchSucceeded(stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long)

  def batchFailed(stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long)
}

private class DetailedOutputMetricsUpdater(outputMetrics: OutputMetrics) extends OutputMetricsUpdater {
  private val mutex = new Semaphore(1)
  private val taskTimer = CassandraConnectorSource.writeTaskTimer.time()

  def batchSucceeded(stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long): Unit = {
    val t = System.nanoTime()
    CassandraConnectorSource.writeBatchTimer.update(t - executionTimestamp, TimeUnit.NANOSECONDS)
    CassandraConnectorSource.writeBatchWaitTimer.update(executionTimestamp - submissionTimestamp, TimeUnit.NANOSECONDS)
    CassandraConnectorSource.writeRowMeter.mark(stmt.rowsCount)
    CassandraConnectorSource.writeByteMeter.mark(stmt.bytesCount)
    CassandraConnectorSource.writeSuccessCounter.inc()
    mutex.acquire()
    outputMetrics.bytesWritten += stmt.bytesCount
    mutex.release()
  }

  def batchFailed(stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long): Unit = {
    CassandraConnectorSource.writeFailureCounter.inc()
  }

  def finish(): Long = {
    val t = taskTimer.stop()
    forceReport()
    t
  }
}

private class DummyOutputMetricsUpdater extends OutputMetricsUpdater {
  private val taskTimer = System.nanoTime()

  def batchSucceeded(stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long): Unit = {}

  def batchFailed(stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long): Unit = {}

  def finish(): Long = {
    System.nanoTime() - taskTimer
  }
}

object OutputMetricsUpdater extends Logging {
  lazy val detailedMetricsEnabled =
    SparkEnv.get.conf.getBoolean("spark.cassandra.output.metrics", defaultValue = true)

  def apply(taskContext: TaskContext, writeConf: WriteConf): OutputMetricsUpdater = {
    CassandraConnectorSource.ensureInitialized

    if (detailedMetricsEnabled || writeConf.throttlingEnabled) {

      if (!detailedMetricsEnabled) {
        logWarning(s"Output metrics updater disabled, but write throughput limiting requested to ${writeConf.throughputMiBPS} MiB/s." +
          s"Enabling output metrics updater, because it is required by throughput limiting.")
      }

      val tm = taskContext.taskMetrics()
      if (tm.outputMetrics.isEmpty || tm.outputMetrics.get.writeMethod != DataWriteMethod.Hadoop)
        tm.outputMetrics = Some(new OutputMetrics(DataWriteMethod.Hadoop))

      new DetailedOutputMetricsUpdater(tm.outputMetrics.get)
    } else {
      new DummyOutputMetricsUpdater()
    }
  }

}