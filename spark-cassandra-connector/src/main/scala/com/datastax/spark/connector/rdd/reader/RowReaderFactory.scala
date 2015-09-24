package com.datastax.spark.connector.rdd.reader

import java.io.Serializable

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.{ColumnRef, CassandraRow}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.util.MagicalTypeTricks.{DoesntHaveImplicit, IsNotSubclassOf}

import scala.annotation.implicitNotFound
import scala.reflect.runtime.universe._


/** Creates [[RowReader]] objects prepared for reading rows from the given Cassandra table. */
@implicitNotFound("No RowReaderFactory can be found for this type")
trait RowReaderFactory[T] {
  def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[T]
  def targetClass: Class[T]
}

/** Helper for implementing `RowReader` objects that can be used as `RowReaderFactory` objects. */
trait ThisRowReaderAsFactory[T] extends RowReaderFactory[T] {
  this: RowReader[T] =>
  def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[T] = this
}

trait LowPriorityRowReaderFactoryImplicits {

  trait IsSingleColumnType[T]
  implicit def isSingleColumnType[T](
    implicit
      ev1: TypeConverter[T],
      ev2: T IsNotSubclassOf (_, _),
      ev3: T IsNotSubclassOf (_, _, _)): IsSingleColumnType[T] = null

  implicit def classBasedRowReaderFactory[R <: Serializable](
    implicit
      tt: TypeTag[R],
      cm: ColumnMapper[R],
      ev: R DoesntHaveImplicit IsSingleColumnType[R]): RowReaderFactory[R]  =
    new ClassBasedRowReaderFactory[R]

  implicit def valueRowReaderFactory[T](
    implicit
      ev1: TypeConverter[T],
      ev2: IsSingleColumnType[T]): RowReaderFactory[T] =
    new ValueRowReaderFactory[T]()
}

object RowReaderFactory extends LowPriorityRowReaderFactoryImplicits {

  /** Default `RowReader`: reads a `Row` into serializable [[CassandraRow]] */
  implicit object GenericRowReader$
    extends RowReader[CassandraRow] with ThisRowReaderAsFactory[CassandraRow] {

    override def targetClass: Class[CassandraRow] = classOf[CassandraRow]

    override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) = {
      assert(row.getColumnDefinitions.size() == columnNames.size,
        "Number of columns in a row must match the number of columns in the table metadata")
      CassandraRow.fromJavaDriverRow(row, columnNames)
    }

    override def neededColumns: Option[Seq[ColumnRef]] = None
  }

}
