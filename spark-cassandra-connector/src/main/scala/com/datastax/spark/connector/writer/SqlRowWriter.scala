package com.datastax.spark.connector.writer

import com.datastax.driver.core.PreparedStatement
import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.catalyst.expressions.Row
/** A [[RowWriter]] that can write [[Row]] objects.*/
class SqlRowWriter(table: TableDef, selectedColumns: Seq[String]) extends RowWriter[Row] {

  override def columnNames =
    selectedColumns.toIndexedSeq

  private def getColumnValue(data: Row, columnName: String): AnyRef = {
    val index = columnNames.indexOf(columnName)
    if (index >= 0 && index < data.size) {
      val converter = table.columnByName(columnName).columnType.converterToCassandra
      val value = data.apply(index)
      if (value == null) null else converter.convert(value).asInstanceOf[AnyRef]
    }
    else
      null
  }

  @transient
  private lazy val buffer = new ThreadLocal[Array[AnyRef]] {
    override def initialValue() = Array.ofDim[AnyRef](columnNames.size)
  }

  private def fillBuffer(data: Row): Array[AnyRef] = {
    val buf = buffer.get
    for (i <- 0 until columnNames.size)
      buf(i) = getColumnValue(data, columnNames(i))
    buf
  }

  override def bind(data: Row, stmt: PreparedStatement) = {
    stmt.bind(fillBuffer(data): _*)
  }

  override def estimateSizeInBytes(data: Row) = {
    ObjectSizeEstimator.measureSerializedSize(fillBuffer(data))
  }
}


object SqlRowWriter {

  object Factory extends RowWriterFactory[Row] {
    override def rowWriter(table: TableDef, columnNames: Seq[String]) =
      new SqlRowWriter(table, columnNames)
  }

}