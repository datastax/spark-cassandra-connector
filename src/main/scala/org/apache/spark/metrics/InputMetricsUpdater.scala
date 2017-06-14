package org.apache.spark.metrics

import org.apache.spark.TaskContext
import org.apache.spark.executor.{DataReadMethod, InputMetrics}

import com.datastax.driver.core.Row
import com.datastax.spark.connector.rdd.ReadConf

/** A trait that provides a method to update read metrics which are collected for connector related tasks.
  * The appropriate instance is created by the companion object.
  *
  * Instances of this trait are not thread-safe. They do not need to because a single instance should be
  * created for each Cassandra read task. This remains valid as long as Cassandra read tasks are
  * single-threaded.
  */
sealed trait InputMetricsUpdater extends MetricsUpdater {
  /** Updates the metrics being collected for the connector after reading each single row. This method
    * is not thread-safe.
    *
    * @param row the row which has just been read
    */
  def updateMetrics(row: Row): Row = row

  /** For internal use only */
  private[metrics] def updateTaskMetrics(count: Int, dataLength: Int): Unit = {}

  /** For internal use only */
  private[metrics] def updateCodahaleMetrics(count: Int, dataLength: Int): Unit = {}
}

object InputMetricsUpdater {
  val DefaultGroupSize = 100

  /** Creates the appropriate instance of `InputMetricsUpdater`.
    *
    * If [[com.datastax.spark.connector.rdd.ReadConf.taskMetricsEnabled ReadConf.taskMetricsEnabled]]
    * is `true`, the created instance will be updating task metrics so
    * that Spark will report them in the UI. Remember that this is supported for Spark 1.2+.
    *
    * If [[org.apache.spark.metrics.CassandraConnectorSource CassandraConnectorSource]]
    * is registered in Spark metrics system, the created instance will be
    * updating the included Codahale metrics. In order to register `CassandraConnectorSource` you need
    * to add it to the metrics configuration file.
    *
    * @param taskContext task context of a task for which this metrics updater is created
    * @param readConf read configuration
    * @param groupSize allows to update Codahale metrics every the given number of rows in order to
    *                  decrease overhead
    */
  def apply(
    taskContext: TaskContext,
    readConf: ReadConf,
    groupSize: Int = DefaultGroupSize
  ): InputMetricsUpdater = {

    val source = MetricsUpdater.getSource(taskContext)

    if (readConf.taskMetricsEnabled) {
      val tm = taskContext.taskMetrics()
      val inputMetrics = tm.inputMetrics

      if (source.isDefined)
        new CodahaleAndTaskMetricsUpdater(groupSize, source.get, inputMetrics)
      else
        new TaskMetricsUpdater(groupSize, inputMetrics)

    } else {
      if (source.isDefined)
        new CodahaleMetricsUpdater(groupSize, source.get)
      else
        new DummyInputMetricsUpdater()
    }
  }

  private abstract class CumulativeInputMetricsUpdater(groupSize: Int)
    extends InputMetricsUpdater with Timer {

    require(groupSize > 0)

    private var cnt = 0
    private var dataLength = 0

    def getRowBinarySize(row: Row) = {
      var size = 0
      for (i <- 0 until row.getColumnDefinitions.size() if !row.isNull(i))
        size += row.getBytesUnsafe(i).remaining()
      size
    }

    override def updateMetrics(row: Row): Row = {
      val binarySize = getRowBinarySize(row)

      // updating task metrics is cheap
      updateTaskMetrics(1, binarySize)

      cnt += 1
      dataLength += binarySize
      if (cnt == groupSize) {
        // Codahale metrics introduce some overhead so in order to minimize it we can update them not
        // that often
        updateCodahaleMetrics(cnt, dataLength)
        cnt = 0
        dataLength = 0
      }
      row
    }

    def finish(): Long = {
      updateCodahaleMetrics(cnt, dataLength)
      val t = stopTimer()
      t
    }
  }

  private trait CodahaleMetricsSupport extends InputMetricsUpdater {
    val source: CassandraConnectorSource

    @inline
    override def updateCodahaleMetrics(count: Int, dataLength: Int): Unit = {
      source.readByteMeter.mark(dataLength)
      source.readRowMeter.mark(count)
    }

    val timer = source.readTaskTimer.time()
  }

  private trait TaskMetricsSupport extends InputMetricsUpdater {
    val inputMetrics: InputMetrics

    @inline
    override def updateTaskMetrics(count: Int, dataLength: Int): Unit = {
      inputMetrics.incBytesRead(dataLength)
      inputMetrics.incRecordsRead(count)
    }
  }

  /** The implementation of [[InputMetricsUpdater]] which does not update anything. */
  private class DummyInputMetricsUpdater extends InputMetricsUpdater with SimpleTimer {
    def finish(): Long = stopTimer()
  }

  /** The implementation of [[InputMetricsUpdater]] which updates only task metrics. */
  private class TaskMetricsUpdater(groupSize: Int, val inputMetrics: InputMetrics)
    extends CumulativeInputMetricsUpdater(groupSize) with TaskMetricsSupport with SimpleTimer

  /** The implementation of [[InputMetricsUpdater]] which updates only Codahale metrics defined in
    * [[CassandraConnectorSource]]. */
  private class CodahaleMetricsUpdater(groupSize: Int, val source: CassandraConnectorSource)
    extends CumulativeInputMetricsUpdater(groupSize) with CodahaleMetricsSupport with CCSTimer

  /** The implementation of [[InputMetricsUpdater]] which updates both Codahale and task metrics. */
  private class CodahaleAndTaskMetricsUpdater(
      groupSize: Int,
      val source: CassandraConnectorSource,
      val inputMetrics: InputMetrics)
    extends CumulativeInputMetricsUpdater(groupSize)
    with TaskMetricsSupport
    with CodahaleMetricsSupport
    with CCSTimer

}