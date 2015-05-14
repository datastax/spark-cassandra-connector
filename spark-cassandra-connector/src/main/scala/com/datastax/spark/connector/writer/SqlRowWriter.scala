package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.catalyst.expressions.Row

/** A [[RowWriter]] that can write SparkSQL `Row` objects. */
class SqlRowWriter(val table: TableDef, val columnNames: Seq[String]) extends RowWriter[Row] {

  private val columns = columnNames.map(table.columnByName).toIndexedSeq
  private val converters = columns.map(_.columnType.converterToCassandra)

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  override def readColumnValues(row: Row, buffer: Array[Any]) = {
    require(row.size == columnNames.size, s"Invalid row size: ${row.size} instead of ${columnNames.size}.")
    for (i <- row.indices)
      buffer(i) = converters(i).convert(row(i))

  }
}


object SqlRowWriter {

  object Factory extends RowWriterFactory[Row] {
    override def rowWriter(table: TableDef, columnNames: Seq[String], aliasToColumnName: Map[String, String]) =
      new SqlRowWriter(table, columnNames)
  }

}