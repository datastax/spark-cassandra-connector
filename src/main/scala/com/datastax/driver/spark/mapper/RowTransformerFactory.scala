package com.datastax.driver.spark.mapper

import com.datastax.driver.core.Row
import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.rdd.CassandraRow

import scala.reflect.runtime.universe._

/** The main reason behind this class is to provide a way to pass TableDef to RowTransformer */ 
trait RowTransformerFactory[T] {
  def rowTransformer(tableDef: TableDef): RowTransformer[T]
}

/** Helper for implementing RowTransformers that can be used as RowTransformerFactories */
trait ThisRowTransformerAsFactory[T] extends RowTransformerFactory[T] {
  this: RowTransformer[T] =>
  def rowTransformer(tableDef: TableDef): RowTransformer[T] = this  
}

trait LowPriorityRowTransformerFactoryImplicits {

  implicit def classBasedRowTransformerFactory[R <: Serializable : TypeTag : ColumnMapper] =
    new ClassBasedRowTransformerFactory[R]
}

object RowTransformerFactory extends LowPriorityRowTransformerFactoryImplicits {

  /** Default row transformer: transforms a {{{Row}}} into serializable {{{CassandraRow}}} */
  implicit object GenericRowTransformer
    extends RowTransformer[CassandraRow] with ThisRowTransformerAsFactory[CassandraRow] {

    override def transform(row: Row, columnNames: Array[String]) = {
      assert(row.getColumnDefinitions.size() == columnNames.size,
        "Number of columns in a row must match the number of columns in the table metadata")
      CassandraRow.fromJavaDriverRow(row, columnNames)
    }

    override def columnCount = None
    override def columnNames = None
  }

}


