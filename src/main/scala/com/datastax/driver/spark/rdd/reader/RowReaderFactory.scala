package com.datastax.driver.spark.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.mapper.ColumnMapper

import scala.reflect.runtime.universe._

/** The main reason behind this class is to provide a way to pass TableDef to RowTransformer */ 
trait RowReaderFactory[T] {
  def rowReader(tableDef: TableDef): RowReader[T]
}

/** Helper for implementing RowTransformers that can be used as RowTransformerFactories */
trait ThisRowReaderAsFactory[T] extends RowReaderFactory[T] {
  this: RowReader[T] =>
  def rowReader(tableDef: TableDef): RowReader[T] = this
}

trait LowPriorityRowReaderFactoryImplicits {

  implicit def classBasedRowReaderFactory[R <: Serializable : TypeTag : ColumnMapper] =
    new ClassBasedRowReaderFactory[R]
}

object RowReaderFactory extends LowPriorityRowReaderFactoryImplicits {

  /** Default row transformer: transforms a `Row` into serializable [[CassandraRow]] */
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


