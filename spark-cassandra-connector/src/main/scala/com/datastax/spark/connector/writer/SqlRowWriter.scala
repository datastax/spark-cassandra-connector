package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.catalyst.expressions.Row

import scala.reflect.ClassTag

/** A [[RowWriter]] that can write SparkSQL [[Row]] objects. */
class SqlRowWriter(val table: TableDef, val columnNames: Seq[String]) extends RowWriter[Row] {

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  override def readColumnValues(row: Row, buffer: Array[Any]) = {
    require(row.size == columnNames.size, s"Invalid row size: ${row.size} instead of ${columnNames.size}.")
    for (i <- 0 until row.size)
      buffer(i) = row(i)
  }
}


object SqlRowWriter {

  object Factory extends RowWriterFactory[Row] {
    override def rowWriter(table: TableDef, columnNames: Seq[String], aliasToColumnName: Map[String, String]) =
      new SqlRowWriter(table, columnNames)

    override def classTag: ClassTag[Row] = ClassTag[Row](Row.getClass)
  }

}
