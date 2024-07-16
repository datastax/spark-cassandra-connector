package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.writer.{TableWriter, WriteConf}
import com.datastax.spark.connector.{ColumnName, SomeColumns}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.metrics.OutputMetricsUpdater
import org.apache.spark.TaskContext

case class CassandraDriverDataWriterFactory(
  connector: CassandraConnector,
  tableDef: TableDef,
  inputSchema: StructType,
  writeConf: WriteConf)
  extends DataWriterFactory
    with StreamingDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = getWriter

  private def getWriter = CassandraDriverDataWriter(connector, tableDef, inputSchema, writeConf)

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = getWriter
}

/*
Since the AsyncWriter here is not Serializable we need to place this code in a a separate object than the
factory above.
 */
case class CassandraDriverDataWriter(
  connector: CassandraConnector,
  tableDef: TableDef,
  inputSchema: StructType,
  writeConf: WriteConf) extends DataWriter[InternalRow] {

  private val unsafeRowWriterFactory = new InternalRowWriterFactory(inputSchema)

  private val columns = SomeColumns(inputSchema.fieldNames.map(name => ColumnName(name)): _*)

  private lazy val metricsUpdater = OutputMetricsUpdater(TaskContext.get(), writeConf)

  private val asycWriter =
    TableWriter(connector, tableDef, columns, writeConf, false)(unsafeRowWriterFactory)
      .getAsyncWriter()

  private val writer = asycWriter.copy(
      successHandler = Some(metricsUpdater.batchFinished(success = true, _, _, _)),
      failureHandler = Some(metricsUpdater.batchFinished(success = false, _, _, _)))

  override def write(record: InternalRow): Unit = writer.write(record)

  override def commit(): WriterCommitMessage = {
    metricsUpdater.finish()
    writer.close()
    CassandraCommitMessage()
  }

  override def abort(): Unit = {
    metricsUpdater.finish()
    writer.close()
  }

  override def close(): Unit = {
    metricsUpdater.finish()
    //Our proxy Session Handler handles double closes by ignoring them so this is fine
    writer.close()
  }
}

case class CassandraCommitMessage() extends WriterCommitMessage
