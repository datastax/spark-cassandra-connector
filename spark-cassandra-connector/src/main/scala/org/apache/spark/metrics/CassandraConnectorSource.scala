package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

/** This class is a Source implementation for Cassandra Connector related Codahale metrics. It is
  * automatically instantiated and registered by Spark metrics system if this source is specified in
  * metrics configuration file.
  *
  * Spark instantiates this class when `SparkEnv` is started. There can be
  * only a single instance of `SparkEnv` so there can be at most a single
  * active instance of `CassandraConnectorSource`. The active instance is assigned to
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
  private var _state: (Option[CassandraConnectorSource], Option[SparkEnv]) = (None, None)

  /** Returns an active instance of `CassandraConnectorSource` if it has been associated with the
    * current `SparkEnv` and the current `SparkEnv` is running.
    */
  def instance = {
    val curEnv = Option(SparkEnv.get)
    val (_instance, _env) = _state
    // this simple check allows to control whether the CassandraConnectorSource was created for this
    // particular SparkEnv. If not - we do not report it as enabled.
    // if we are in the executor environment (_env == None) we do not check for the current env
    if (_env.isEmpty || (curEnv.isDefined && !curEnv.get.isStopped && (curEnv.orNull eq _env.orNull)))
      _instance
    else
      None
  }

  /** This method sets the given instance of `CassandraConnectorSource` as the current active instance.
    * It is allowed to call this method only once for the same `SparkEnv` instance which is currently
    * up and running or this is the executor environment.
    */
  private def maybeSetInstance(ccs: CassandraConnectorSource): Unit = synchronized {
    val curEnv = Option(SparkEnv.get)
    val (_instance, _env) = _state
    assert(ccs != null, "ccs cannot be null")

    // when it is initialized in the executor environment, SparkEnv is not set when this method is called
    // therefore, we have to consider two cases:
    //  - when it is called from executor for the first time, curEnv is null so _instance is set to some
    //    and _env is set to null - this combination of (_env, _instance) == (null, Some) may only exist
    //    in the executor environment where only one instance of this metrics source can be created
    //
    //  - when it is called from the driver, curEnv always references the current SparkEnv and it cannot
    //    be null. Therefore the pair (_env, _instance) == (not null, Some) is possible only in the
    //    driver environment

    if (_env.isEmpty && _instance.isEmpty) { // called for the first time
      curEnv.foreach(env ⇒ assert(!env.isStopped, "SparkEnv must be up and running"))
      // when it is initialised in the executor env, SparkEnv is not yet set - nothing to be asserted
    } else {
      // if we are in the driver env and the method is not called for the first time, _env is not null
      // if we are in the executor env, this method can be called only once - therefore assertion fail
      assert(_env.isDefined, "instance has been already set for this environment")
      curEnv.foreach(env ⇒ assert(!env.isStopped, "SparkEnv must be up and running"))
      assert(curEnv.orNull ne _env.orNull, "instance has been already set for the current SparkEnv")
    }

    _state = (Some(ccs), curEnv)
  }

  /** This method should be used only for testing. Never call it in production. */
  private[metrics] def reset(): Unit = synchronized {
    _state = (None, None)
  }
}
