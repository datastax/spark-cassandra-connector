package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.writer.{RowWriter, RowWriterFactory}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}

class UnsafeRowWriterFactory(expressions: Seq[Expression]) extends RowWriterFactory[UnsafeRow] {
  /** Creates a new `RowWriter` instance.
    *
    * @param table           target table the user wants to write into
    * @param selectedColumns columns selected by the user; the user might wish to write only a
    *                        subset of columns */
  override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]):
  RowWriter[UnsafeRow] = new UnsafeRowWriter(expressions, table, selectedColumns)

}

/**
  * A [[RowWriter]] that can write SparkSQL `UnsafeRow` objects.
  * [[expressions]] needs to be a sequence of already BoundReferences to the incoming UnsafeRows
  **/
class UnsafeRowWriter(
  val expressions: Seq[Expression],
  val table: TableDef,
  val selectedColumns: IndexedSeq[ColumnRef]) extends RowWriter[UnsafeRow] {

  @transient lazy private val keyExtractProj = UnsafeProjection.create(expressions)
  override val columnNames = selectedColumns.map(_.columnName)
  private val columns = columnNames.map(table.columnByName)
  private val columnTypes = columns.map(_.columnType)
  private val convertersToScala = expressions.map(expression =>
    CatalystTypeConverters.createToScalaConverter(expression.dataType)
  )
  private val convertersToCassandra = columns.map(_.columnType.converterToCassandra)
  private val converters = convertersToScala.zip(convertersToCassandra).map(converterPair =>
    converterPair._1.andThen(converterPair._2.convert(_))
  )
  private val dataTypes = expressions.map(_.dataType)
  private val size = expressions.size

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  override def readColumnValues(row: UnsafeRow, buffer: Array[Any]) = {
    val myRow = keyExtractProj(row)
    var i = 0;
    // Using while loop to avoid allocations in for each
    while (i < size) {
      val colValue = myRow.get(i, dataTypes(i))
      buffer(i) = converters(i).apply(colValue)
      i += 1
    }
  }
}


