package com.datastax.spark.connector.writer

import scala.reflect.runtime.universe._
import scala.collection.Seq
import com.datastax.spark.connector.{CassandraRow, ColumnRef}
import com.datastax.spark.connector.cql.{StructDef, TableDef}
import com.datastax.spark.connector.mapper.{ColumnMapper, MappedToGettableDataConverter, TableDescriptor}
import com.datastax.spark.connector.types.TypeConverter

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : TypeTag : ColumnMapper](
    table: Either[TableDef,TableDescriptor],
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[T] {

  /* Explicit type conversion here because we need a StructDef type with a ValueRepr of CassandraRow */
  private val converter:TypeConverter[CassandraRow] = table match {
    case Left(x) => MappedToGettableDataConverter[T](x, selectedColumns)
    case Right(x) => MappedToGettableDataConverter[T](x, selectedColumns)
  }
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
      new DefaultRowWriter[T](Left(tableDef), selectedColumns)
    }
  }
}

