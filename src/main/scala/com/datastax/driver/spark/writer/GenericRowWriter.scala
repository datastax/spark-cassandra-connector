package com.datastax.driver.spark.writer

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.spark.CassandraRow
import com.datastax.driver.spark.connector.TableDef

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class GenericRowWriter(table: TableDef, selectedColumns: Seq[String]) extends RowWriter[CassandraRow] {

  override def columnNames =
    selectedColumns.toIndexedSeq

  private def getColumnValue(data: CassandraRow, columnName: String): AnyRef = {
    val converter = table.columnByName(columnName).columnType.converterToCassandra
    val value = data.get[AnyRef](columnName)
    converter.convert(value).asInstanceOf[AnyRef]
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
