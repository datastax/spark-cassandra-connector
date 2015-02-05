package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

object CassandraConnectorSource extends Source {
  override val sourceName = "cassandra-connector"

  override val metricRegistry = new metrics.MetricRegistry

  val writeByteMeter = metricRegistry.meter("write-byte-meter")
  val writeRowMeter = metricRegistry.meter("write-row-meter")
  val writeBatchTimer = metricRegistry.timer("write-batch-timer")
  val writeBatchWaitTimer = metricRegistry.timer("write-batch-wait-timer")
  val writeTaskTimer = metricRegistry.timer("write-task-timer")
  
  val writeSuccessCounter = metricRegistry.counter("write-success-counter")
  val writeFailureCounter = metricRegistry.counter("write-failure-counter")

  val readByteMeter = metricRegistry.meter("read-byte-meter")
  val readRowMeter = metricRegistry.meter("read-row-meter")
  val readMoreRowsTimer = metricRegistry.timer("read-more-rows-timer")
  val readTaskTimer = metricRegistry.timer("read-task-timer")

  lazy val ensureInitialized = SparkEnv.get.metricsSystem.registerSource(this)
}
