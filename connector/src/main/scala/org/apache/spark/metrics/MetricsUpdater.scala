/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
