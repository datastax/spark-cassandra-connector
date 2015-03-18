package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

/** This class is a Source implementation for Cassandra Connector related Codahale metrics. It is
  * automatically instantiated and registered by Spark metrics system if this source is specified in
  * metrics configuration file.
  *
  * Spark instantiates this class when [[org.apache.spark.SparkEnv SparkEnv]] is started. There can be
  * only a single instance of [[org.apache.spark.SparkEnv SparkEnv]] so there can be at most a single
  * active instance of [[CassandraConnectorSource]]. The active instance is assigned to
  * `CassandraConnectorSource._instance` so that it can be retrieved from anywhere by
  * [[CassandraConnectorSource.instance]] method. We need this because we have to access the meters from
  * the task execution.
  */
class CassandraConnectorSource extends Source {
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
  val readTaskTimer = metricRegistry.timer("read-task-timer")

  CassandraConnectorSource.maybeSetInstance(this)
}

object CassandraConnectorSource {
  @volatile
  private var _instance: Option[CassandraConnectorSource] = None
  @volatile
  private var _env: SparkEnv = null

  /** Returns an active instance of [[CassandraConnectorSource]] if it has been associated with the
    * current [[org.apache.spark.SparkEnv SparkEnv]] and the current [[org.apache.spark.SparkEnv SparkEnv]]
    * is running.
    */
  def instance = {
    val curEnv = SparkEnv.get
    // this simple check allows to control whether the CassandraConnectorSource was created for this
    // particular SparkEnv. If not - we do not report it as enabled.
    if (curEnv != null && !curEnv.isStopped && (curEnv eq _env))
      _instance
    else
      None
  }

  /** This method sets the given instance of [[CassandraConnectorSource]] as the current active instance.
    * It is allowed to call this method only once for the same `SparkEnv` instance which is currently
    * up and running.
    */
  private def maybeSetInstance(ccs: CassandraConnectorSource): Unit = synchronized {
    val curEnv = SparkEnv.get
    assert(ccs != null, "ccs cannot be null")
    assert(curEnv != null && !curEnv.isStopped, "SparkEnv must be up and running")
    assert(curEnv ne _env, "instance has been already set for the current SparkEnv")

    CassandraConnectorSource._instance = Some(ccs)
    CassandraConnectorSource._env = curEnv
  }

}
