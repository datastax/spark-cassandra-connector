package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ProtocolVersion, PreparedStatement}
import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.catalyst.expressions.Row

/** A [[RowWriter]] that can write [[Row]] objects.*/
abstract class AbstractRowWriter[T <: Row](table: TableDef, selectedColumns: Seq[String]) extends RowWriter[T] {

  override def columnNames =
    selectedColumns.toIndexedSeq

  protected def getColumnValue(data: T, columnName: String): AnyRef

  @transient
  protected lazy val buffer = new ThreadLocal[Array[AnyRef]] {
    override def initialValue() = Array.ofDim[AnyRef](columnNames.size)
  }

  protected def fillBuffer(data: T): Array[AnyRef] = {
    val buf = buffer.get
    for (i <- 0 until columnNames.size)
      buf(i) = getColumnValue(data, columnNames(i))
    buf
  }

  override def bind(data: T, stmt: PreparedStatement, protocolVersion: ProtocolVersion) = {
    stmt.bind(fillBuffer(data): _*)
  }

  override def estimateSizeInBytes(data: T) = {
    ObjectSizeEstimator.measureSerializedSize(fillBuffer(data))
  }
}
