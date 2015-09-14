package com.datastax.spark.connector.rdd.reader

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import com.datastax.driver.core.Row

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.util.JavaApiHelper


/** Transforms a Cassandra Java driver `Row` into an object of a user provided class,
  * calling the class constructor */
final class ClassBasedRowReader[R : TypeTag : ColumnMapper](
    table: TableDef,
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowReader[R] {

  private val converter =
    new GettableDataToMappedTypeConverter[R](table, selectedColumns)

  private val isReadingTuples =
    TypeTag.synchronized(typeTag[R].tpe.typeSymbol.fullName startsWith "scala.Tuple")

  override val neededColumns = {
    val ctorRefs = converter.columnMap.constructor
    val setterRefs = converter.columnMap.setters.values
    Some(ctorRefs ++ setterRefs)
  }

  override def read(row: Row, ignored: Array[String]): R = {
    // can't use passed array of column names, because it is already after applying aliases
    val columnNames = row.getColumnDefinitions.iterator.map(_.getName).toArray
    val cassandraRow = CassandraRow.fromJavaDriverRow(row, columnNames)
    converter.convert(cassandraRow)
  }
}


class ClassBasedRowReaderFactory[R : TypeTag : ColumnMapper] extends RowReaderFactory[R] {

  def columnMapper = implicitly[ColumnMapper[R]]

  override def rowReader(tableDef: TableDef, selection: IndexedSeq[ColumnRef]) =
    new ClassBasedRowReader[R](tableDef, selection)

  override def targetClass: Class[R] = JavaApiHelper.getRuntimeClass(typeTag[R])
}
