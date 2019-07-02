/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.spark.reporting

import java.util
import java.util.Date

import scala.collection.mutable

import com.codahale.metrics.{Gauge, Metric, MetricSet}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.StageInfo

import scala.collection.JavaConverters._

object DseSparkContextMetrics extends MetricSet {

  case class JobInfo(numStages: Int, numTasks: Int, scheduleTime: Date, stagesIds: Seq[Int])

  private[reporting] val totalMetrics = new AppMetrics
  private[reporting] val wastedMetrics = new AppMetrics

  private[reporting] val activeJobs = new mutable.HashMap[Int, JobInfo]()
  private[reporting] var succeededJobs = 0
  private[reporting] var failedJobs = 0

  private[reporting] val activeStages = new mutable.HashMap[(Int, Int), StageInfo]()
  private[reporting] var succeededStages = 0
  private[reporting] var failedStages = 0
  private[reporting] var skippedStages = 0

  private[reporting] var succeededTasks = 0
  private[reporting] var failedTasks = 0

  private[reporting] var resubmittedTasks = 0
  private[reporting] var fetchFailedTasks = 0
  private[reporting] var exceptionFailureTasks = 0
  private[reporting] var resultLostTasks = 0
  private[reporting] var killedTasks = 0
  private[reporting] var executorLostTasks = 0
  private[reporting] var unknownFailureTasks = 0


  def reset(): Unit = {
    totalMetrics.reset()
    wastedMetrics.reset()

    activeJobs.clear()
    succeededJobs = 0
    failedJobs = 0

    activeStages.clear()
    succeededStages = 0
    failedStages = 0
    skippedStages = 0

    succeededTasks = 0
    failedTasks = 0

    resubmittedTasks = 0
    fetchFailedTasks = 0
    exceptionFailureTasks = 0
    resultLostTasks = 0
    killedTasks = 0
    executorLostTasks = 0
    unknownFailureTasks = 0
  }

  private[reporting] class AppMetrics {
    @volatile var diskBytesSpilled = 0L
    @volatile var memoryBytesSpilled = 0L
    @volatile var executorDeserializeTime = 0L
    @volatile var executorRunTime = 0L
    @volatile var jvmGCTime = 0L
    @volatile var resultSerializationTime = 0L
    @volatile var resultSize = 0L
    @volatile var shuffleRemoteBlocksFetched = 0L
    @volatile var shuffleLocalBlocksFetched = 0L
    @volatile var shuffleFetchWaitTime = 0L
    @volatile var shuffleRemoteBytesRead = 0L
    @volatile var shuffleLocalBytesRead = 0L
    @volatile var shuffleBytesWritten = 0L
    @volatile var shuffleWriteTime = 0L
    @volatile var totalWrittenBytes = 0L
    @volatile var totalWrittenRecords = 0L
    @volatile var totalReadBytes = 0L
    @volatile var totalReadRecords = 0L
    @volatile var shuffleRecordsRead = 0L
    @volatile var shuffleRecordsWritten = 0L

    def gauges(prefix: String): Map[String, Metric] = {
      Map(
        gauge(prefix + "diskBytesSpilled", diskBytesSpilled),
        gauge(prefix + "memoryBytesSpilled", memoryBytesSpilled),
        gauge(prefix + "executorDeserializeTime", executorDeserializeTime),
        gauge(prefix + "executorRunTime", executorRunTime),
        gauge(prefix + "jvmGCTime", jvmGCTime),
        gauge(prefix + "resultSerializationTime", resultSerializationTime),
        gauge(prefix + "resultSize", resultSize),
        gauge(prefix + "shuffleRemoteBlocksFetched", shuffleRemoteBlocksFetched),
        gauge(prefix + "shuffleLocalBlocksFetched", shuffleLocalBlocksFetched),
        gauge(prefix + "shuffleFetchWaitTime", shuffleFetchWaitTime),
        gauge(prefix + "shuffleRemoteBytesRead", shuffleRemoteBytesRead),
        gauge(prefix + "shuffleLocalBytesRead", shuffleLocalBytesRead),
        gauge(prefix + "shuffleBytesWritten", shuffleBytesWritten),
        gauge(prefix + "shuffleWriteTime", shuffleWriteTime),
        gauge(prefix + "totalWrittenBytes", totalWrittenBytes),
        gauge(prefix + "totalReadBytes", totalReadBytes),
        gauge(prefix + "shuffleRecordsRead", shuffleRecordsRead),
        gauge(prefix + "shuffleRecordsWritten", shuffleRecordsWritten),
        gauge(prefix + "totalReadRecords", totalReadRecords),
        gauge(prefix + "totalWrittenRecords", totalWrittenRecords)
      )
    }

    def reset(): Unit = {
      diskBytesSpilled = 0L
      memoryBytesSpilled = 0L
      executorDeserializeTime = 0L
      executorRunTime = 0L
      jvmGCTime = 0L
      resultSerializationTime = 0L
      resultSize = 0L
      shuffleRemoteBlocksFetched = 0L
      shuffleLocalBlocksFetched = 0L
      shuffleFetchWaitTime = 0L
      shuffleRemoteBytesRead = 0L
      shuffleLocalBytesRead = 0L
      shuffleBytesWritten = 0L
      shuffleWriteTime = 0L
      totalWrittenBytes = 0L
      totalReadBytes = 0L
      shuffleRecordsRead = 0L
      shuffleRecordsWritten = 0L
      totalReadRecords = 0L
      totalWrittenRecords = 0L
    }

    def +=(m: TaskMetrics): AppMetrics = {
      diskBytesSpilled += m.diskBytesSpilled
      memoryBytesSpilled += m.memoryBytesSpilled
      executorDeserializeTime += m.executorDeserializeTime
      executorRunTime += m.executorRunTime
      jvmGCTime += m.jvmGCTime
      resultSerializationTime += m.resultSerializationTime
      resultSize += m.resultSize

      shuffleRemoteBlocksFetched += m.shuffleReadMetrics.remoteBlocksFetched
      shuffleLocalBlocksFetched += m.shuffleReadMetrics.localBlocksFetched
      shuffleRemoteBytesRead += m.shuffleReadMetrics.remoteBytesRead
      shuffleLocalBytesRead += m.shuffleReadMetrics.localBytesRead
      shuffleFetchWaitTime += m.shuffleReadMetrics.fetchWaitTime
      shuffleRecordsRead += m.shuffleReadMetrics.recordsRead

      shuffleBytesWritten += m.shuffleWriteMetrics.bytesWritten
      shuffleWriteTime += m.shuffleWriteMetrics.writeTime
      shuffleRecordsWritten += m.shuffleWriteMetrics.recordsWritten

      totalReadBytes += m.inputMetrics.bytesRead
      totalReadRecords += m.inputMetrics.recordsRead

      totalWrittenBytes += m.outputMetrics.bytesWritten
      totalWrittenRecords += m.outputMetrics.recordsWritten

      this
    }
  }

  override def getMetrics: util.Map[String, Metric] = {
    (Map(
      gauge("activeJobsNumber", activeJobs.size),
      gauge("succeededJobsNumber", succeededJobs),
      gauge("failedJobsNumber", failedJobs),
      gauge("activeStagesNumber", activeStages.size),
      gauge("succeededStagesNumber", succeededStages),
      gauge("failedStagesNumber", failedStages),
      gauge("skippedStagesNumber", skippedStages),
      gauge("resubmittedTasksNumber", resubmittedTasks),
      gauge("fetchFailedTasksNumber", fetchFailedTasks),
      gauge("exceptionFailureTasksNumber", exceptionFailureTasks),
      gauge("resultLostTasksNumber", resultLostTasks),
      gauge("killedTasksNumber", killedTasks),
      gauge("executorLostTasksNumber", executorLostTasks),
      gauge("unknownFailureTasksNumber", unknownFailureTasks)
    ) ++ totalMetrics.gauges("all.") ++ wastedMetrics.gauges("wasted.")).asJava
  }

  private def gauge[T](name: String, value: => T): (String, Gauge[T]) = {
    name -> new Gauge[T]() {
      override def getValue: T = value
    }
  }

}
