package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.metrics.source.Source

/** This class is a Source implementation for Cassandra Connector related Codahale metrics. It is
  * automatically instantiated and registered by Spark metrics system if this source is specified in
  * metrics configuration file.
  */
class CassandraConnectorSource extends Source {
  override val sourceName = "cassandra-connector"

  override val metricRegistry = new metrics.MetricRegistry

  val writeByteMeter = metricRegistry.meter("write-byte-meter")
  val writeRowMeter = metricRegistry.meter("write-row-meter")
  val writeBatchTimer = metricRegistry.timer("write-batch-timer")
  val writeBatchWaitTimer = metricRegistry.timer("write-batch-wait-timer")
  val writeTaskTimer = metricRegistry.timer("write-task-timer")
  val writeBatchSizeHistogram = metricRegistry.histogram("write-batch-size-histogram")

  val writeSuccessCounter = metricRegistry.counter("write-success-counter")
  val writeFailureCounter = metricRegistry.counter("write-failure-counter")

  val readByteMeter = metricRegistry.meter("read-byte-meter")
  val readRowMeter = metricRegistry.meter("read-row-meter")
  val readTaskTimer = metricRegistry.timer("read-task-timer")
}
