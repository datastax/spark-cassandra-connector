package com.datastax.spark.connector.writer

import scala.reflect.runtime.universe._
import scala.collection.Seq

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : TypeTag : ColumnMapper](
    table: TableDef, 
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[T] {

  private val converter = MappedToGettableDataConverter[T](table, selectedColumns)
  override val columnNames = selectedColumns.map(_.columnName)

  override def readColumnValues(data: T, buffer: Array[Any]) = {
    val row = converter.convert(data)
    for (i <- columnNames.indices)
      buffer(i) = row.getRaw(i)
  }
}

object DefaultRowWriter {

  def factory[T : ColumnMapper : TypeTag] = new RowWriterFactory[T] {
    override def rowWriter(tableDef: TableDef, selectedColumns: IndexedSeq[ColumnRef]) = {
      new DefaultRowWriter[T](tableDef, selectedColumns)
    }
  }
}

