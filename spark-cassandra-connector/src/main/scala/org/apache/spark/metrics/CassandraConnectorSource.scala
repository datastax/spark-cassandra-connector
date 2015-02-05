package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

object CassandraConnectorSource extends Source {
  override val sourceName = "cassandra-connector"

  override val metricRegistry = new metrics.MetricRegistry

  val rowsWriteMeter = metricRegistry.meter("write-row-meter")
  val bytesWriteMeter = metricRegistry.meter("write-byte-meter")
  val batchesSuccessCounter = metricRegistry.counter("write-batch-success-counter")
  val batchesFailureCounter = metricRegistry.counter("write-batch-failure-counter")
  val batchesWriteTimer = metricRegistry.timer("write-batch-timer")
  val batchesWaitingTimer = metricRegistry.timer("write-wait-batch-timer")

  lazy val ensureInitialized = SparkEnv.get.metricsSystem.registerSource(this)
}
