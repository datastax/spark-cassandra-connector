package com.datastax.spark.connector.writer

import java.net.InetAddress
import java.util.UUID

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.types.TypeConversionException
import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}


/** A [[RowWriter]] that can write SparkSQL `Row` objects. */
class SqlRowWriter(val table: TableDef, val selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[Row] {

  override val columnNames = selectedColumns.map(_.columnName)
  private val columns = columnNames.map(table.columnByName)
  private val columnTypes = columns.map(_.columnType)
  private val converters = columns.map(_.columnType.converterToCassandra)

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  override def readColumnValues(row: Row, buffer: Array[Any]) = {
    require(row.size == columnNames.size, s"Invalid row size: ${row.size} instead of ${columnNames.size}.")
    for (i <- 0 until row.size) {
      val colValue = row(i)
      buffer(i) = Try(converters(i).convert(colValue)) match {
        case Success(value) => value
        case Failure(ex: IllegalArgumentException) =>
          val columnType = columnTypes(i).scalaTypeName
          throw new TypeConversionException(
            s"Cannot convert value $colValue of column ${columnNames(i)} to type $columnType", ex)
        case Failure(ex) => throw ex
      }
    }
  }
}


object SqlRowWriter {

  object Factory extends RowWriterFactory[Row] {
    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
      new SqlRowWriter(table, selectedColumns)
  }

}
