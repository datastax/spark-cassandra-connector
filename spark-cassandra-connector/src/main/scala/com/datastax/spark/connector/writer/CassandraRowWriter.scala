package com.datastax.spark.connector.writer

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.TableDef

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class CassandraRowWriter(table: TableDef, selectedColumns: Seq[String]) extends RowWriter[CassandraRow] {

  val columnNames = selectedColumns

  private val columns = columnNames.map(table.columnByName).toIndexedSeq
  private val converters = columns.map(_.columnType.converterToCassandra)

  override def readColumnValues(data: CassandraRow, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex) {
      val value = data.getRaw(c)
      val convertedValue = converters(i).convert(value)
      buffer(i) = convertedValue
    }
  }
}


object CassandraRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, columnNames: Seq[String], aliasToColumnName: Map[String, String]) =
      new CassandraRowWriter(table, columnNames)
  }

}
