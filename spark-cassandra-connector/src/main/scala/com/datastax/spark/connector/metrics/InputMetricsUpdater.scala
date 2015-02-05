package com.datastax.spark.connector.metrics

import com.codahale.metrics.Timer
import com.datastax.driver.core.Row
import org.apache.spark.TaskContext
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.metrics.CassandraConnectorSource

class InputMetricsUpdater private(metrics: InputMetrics, groupSize: Int) {
  require(groupSize > 0)

  private val taskTimer = CassandraConnectorSource.partitionReadTimer.time()

  private var cnt = 0
  private var dataLength = metrics.bytesRead

  def updateMetrics(row: Row): Row = {
    for (i <- 0 until row.getColumnDefinitions.size() if !row.isNull(i))
      dataLength += row.getBytesUnsafe(i).remaining()

    cnt += 1
    if (cnt == groupSize)
      update()
    row
  }

  @inline
  private def update(): Unit = {
    CassandraConnectorSource.rowsReadMeter.mark(cnt)
    CassandraConnectorSource.bytesReadMeter.mark(metrics.bytesRead - dataLength)
    dataLength = metrics.bytesRead
    cnt = 0
  }

  def finish(): Long = {
    update()
    taskTimer.stop()
  }
}

object InputMetricsUpdater {
  val resultSetFetchTimer: Option[Timer] = Some(CassandraConnectorSource.fetchMoreRowsTimer)

  def apply(taskContext: TaskContext, groupSize: Int): InputMetricsUpdater = {
    CassandraConnectorSource.ensureInitialized

    if (taskContext.taskMetrics().inputMetrics.isEmpty || taskContext.taskMetrics().inputMetrics.get.readMethod != DataReadMethod.Hadoop)
      taskContext.taskMetrics().inputMetrics = Some(new InputMetrics(DataReadMethod.Hadoop))

    new InputMetricsUpdater(taskContext.taskMetrics().inputMetrics.get, groupSize)
  }
}