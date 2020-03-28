package org.apache.spark.metrics

import com.codahale.metrics.Timer.Context
import org.apache.spark.{SparkEnv, TaskContext}

/** The base trait for metrics updaters implementations. The metrics updater is an object which provides
  * a unified way to update all the relevant metrics which are collected for the particular type of
  * activity. The connector provides `InputMetricsUpdater` and `OutputMetricsUpdater` which are aimed
  * to update all the read and write metrics respectively. */
trait MetricsUpdater {
  /** A method to be called when the task is finished. It stops the task timer and flushes data. */
  def finish(): Long
}

object MetricsUpdater {
  val cassandraConnectorSourceName = "cassandra-connector"

  def getSource(taskContext: TaskContext): Option[CassandraConnectorSource] =
    taskContext
        .getMetricsSources(cassandraConnectorSourceName).headOption
        .map(_.asInstanceOf[CassandraConnectorSource])
}

/** Timer mixin allows to measure the time of a task - or, in other words - the time from creating an
  * instance to calling `com.codahale.metrics.Timer.Context.stop` method. */
trait Timer {
  def stopTimer(): Long
}

trait SimpleTimer extends Timer {
  private val startTime = System.nanoTime()

  override def stopTimer(): Long = System.nanoTime() - startTime
}

trait CCSTimer extends Timer {
  def source: CassandraConnectorSource

  val timer: Context

  override def stopTimer(): Long = {
    val t = timer.stop()
    Option(SparkEnv.get).flatMap(env => Option(env.metricsSystem)).foreach(_.report())
    t
  }
}
