package com.datastax.spark.connector.writer

import com.datastax.driver.core.PreparedStatement
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.TableDef

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class GenericRowWriter(table: TableDef, selectedColumns: Seq[String]) extends RowWriter[CassandraRow] {

  override def columnNames =
    selectedColumns.toIndexedSeq

  private def getColumnValue(data: CassandraRow, columnName: String): AnyRef = {
    val index = data.indexOf(columnName)
    if (index >= 0) {
      val converter = table.columnByName(columnName).columnType.converterToCassandra
      val value = data.get[AnyRef](index)
      converter.convert(value).asInstanceOf[AnyRef]
    }
    else
      null
  }

  @transient
  private lazy val buffer = new ThreadLocal[Array[AnyRef]] {
    override def initialValue() = Array.ofDim[AnyRef](columnNames.size)
  }

  private def fillBuffer(data: CassandraRow): Array[AnyRef] = {
    val buf = buffer.get
    for (i <- 0 until columnNames.size)
      buf(i) = getColumnValue(data, columnNames(i))
    buf
  }

  override def bind(data: CassandraRow, stmt: PreparedStatement) = {
    stmt.bind(fillBuffer(data): _*)
  }

  override def estimateSizeInBytes(data: CassandraRow) = {
    ObjectSizeEstimator.measureSerializedSize(fillBuffer(data))
  }
}


object GenericRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, columnNames: Seq[String]) =
      new GenericRowWriter(table, columnNames)
  }

}
