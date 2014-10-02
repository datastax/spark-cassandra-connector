package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.util.MagicalTypeTricks.{IsNotSubclassOf, DoesntHaveImplicit}

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

  trait IsSingleColumnType[T]
  implicit def isSingleColumnType[T](implicit ev1: TypeConverter[T], ev2: T IsNotSubclassOf (_, _)): IsSingleColumnType[T] = null

  implicit def classBasedRowReaderFactory[R <: Serializable]
      (implicit tt: TypeTag[R], cm: ColumnMapper[R], ev: R IsNotSubclassOf (_, _)): RowReaderFactory[R]  =
    new ClassBasedRowReaderFactory[R]

  implicit def singleColumnKeyValueRowReaderFactory[
      K : TypeTag : TypeConverter : IsSingleColumnType,
      V : TypeTag : TypeConverter : IsSingleColumnType]: RowReaderFactory[(K, V)] =
    new ClassBasedRowReaderFactory[(K, V)]

  implicit def compoundColumnKeyValueRowReaderFactory[K <: Serializable, V <: Serializable]
      (implicit tt1: TypeTag[K], cm1: ColumnMapper[K], ev1: K DoesntHaveImplicit IsSingleColumnType[K],
                tt2: TypeTag[V], cm2: ColumnMapper[V], ev2: V DoesntHaveImplicit IsSingleColumnType[V]): RowReaderFactory[(K, V)] =
    new KeyValueRowReaderFactory[K, V]()

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


