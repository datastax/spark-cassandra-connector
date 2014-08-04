package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

import scala.reflect.runtime.universe._

/** Creates [[RowReader]] objects prepared for reading rows from the given Cassandra table. */
trait RowReaderFactory[T] {
  def rowReader(table: TableDef): RowReader[T]
}

/** Helper for implementing `RowReader` objects that can be used as `RowReaderFactory` objects. */
trait ThisRowReaderAsFactory[T] extends RowReaderFactory[T] {
  this: RowReader[T] =>
  def rowReader(table: TableDef): RowReader[T] = this
}

trait LowPriorityRowReaderFactoryImplicits {

  implicit def classBasedRowReaderFactory[R <: Serializable : TypeTag : ColumnMapper] =
    new ClassBasedRowReaderFactory[R]
}

object RowReaderFactory extends LowPriorityRowReaderFactoryImplicits {

  /** Default `RowReader`: reads a `Row` into serializable [[CassandraRow]] */
  implicit object GenericRowReader$
    extends RowReader[CassandraRow] with ThisRowReaderAsFactory[CassandraRow] {

    override def read(row: Row, columnNames: Array[String]) = {
      assert(row.getColumnDefinitions.size() == columnNames.size,
        "Number of columns in a row must match the number of columns in the table metadata")
      CassandraRow.fromJavaDriverRow(row, columnNames)
    }

    override def columnCount = None
    override def columnNames = None
  }

}


