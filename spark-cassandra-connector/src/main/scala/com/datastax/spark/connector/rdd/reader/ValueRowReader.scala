package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.util.JavaApiHelper

class ValueRowReader[T: TypeConverter](columnRef: ColumnRef) extends RowReader[T] {

  private val converter = implicitly[TypeConverter[T]]

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    * @param row row fetched from Cassandra
    * @param columnNames column names available in the `row` */
  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): T = {
    columnRef match {
      case SelectableColumnRef(selectedAs) => converter.convert(GettableData.get(row, selectedAs))
      case ColumnIndex(idx) => converter.convert(GettableData.get(row, idx))
    }
  }

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  override def columnNames: Option[Seq[String]] = columnRef match {
    case SelectableColumnRef(selectedAs) => Some(Seq(selectedAs))
    case _ => None
  }

  /** The number of columns that need to be fetched from C*. */
  override def requiredColumns: Option[Int] = columnRef match {
    case ColumnIndex(idx) => Some(idx)
    case _ => None
  }

  override def consumedColumns: Option[Int] = Some(1)
}

class ValueRowReaderFactory[T: TypeConverter]
  extends RowReaderFactory[T] {

  override def rowReader(table: TableDef, options: RowReaderOptions): RowReader[T] = {
    new ValueRowReader[T](ColumnIndex(options.offset))
  }

  override def targetClass: Class[T] = JavaApiHelper.getRuntimeClass(implicitly[TypeConverter[T]].targetTypeTag)
}
