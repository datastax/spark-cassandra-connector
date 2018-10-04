/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark.reporting

import java.util.Date

import org.apache.spark._
import org.apache.spark.scheduler._

class DseSparkListener(conf: SparkConf) extends SparkListener {

  import DseSparkContextMetrics._

  DseSparkContextMetrics.reset()

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    activeStages.remove(stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.attemptNumber)
    stageCompleted.stageInfo.failureReason match {
      case Some(failure) => failedStages += 1
      case None => succeededStages += 1
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    activeStages.put((stageSubmitted.stageInfo.stageId, stageSubmitted.stageInfo.attemptNumber), stageSubmitted.stageInfo)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    taskEnd.reason match {
      case Success =>
        succeededTasks += 1
        if (taskEnd.taskMetrics != null) {
          totalMetrics += taskEnd.taskMetrics
        }
      case r: TaskFailedReason =>
        failedTasks += 1
        if (taskEnd.taskMetrics != null) {
          totalMetrics += taskEnd.taskMetrics
          wastedMetrics += taskEnd.taskMetrics
        }
        r match {
          case Resubmitted => resubmittedTasks += 1
          case _: FetchFailed => fetchFailedTasks += 1
          case _: ExceptionFailure => exceptionFailureTasks += 1
          case TaskResultLost => resultLostTasks += 1
          case _: TaskKilled => killedTasks += 1
          case _: ExecutorLostFailure => executorLostTasks += 1
          case _ => unknownFailureTasks += 1
        }
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    activeJobs.put(jobStart.jobId,
      JobInfo(jobStart.stageInfos.length, jobStart.stageInfos.map(_.numTasks).sum, new Date(), jobStart.stageIds))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    activeJobs.remove(jobEnd.jobId) match {
      case Some(jobInfo) =>
        for (stageId <- jobInfo.stagesIds) {
          val stages = activeStages.keySet.filter(_._1 == stageId)
          for (stageKey <- stages; stageInfo <- activeStages.remove(stageKey)) {
            stageInfo.failureReason match {
              case Some(failure) => failedStages += 1
              case None => skippedStages += 1
            }
          }
        }
      case _ =>
    }
    jobEnd.jobResult match {
      case JobSucceeded => succeededJobs += 1
      case _ => failedJobs += 1
    }
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = synchronized {
    reset()
  }

}
