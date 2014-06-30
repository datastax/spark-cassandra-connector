package com.datastax.driver.spark.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.mapper._

import scala.reflect.runtime.universe._

/** Transforms a Cassandra Java driver `Row` into an object of a user provided class, calling the class constructor */
class ClassBasedRowReader[R : TypeTag : ColumnMapper](tableDef: TableDef) extends RowReader[R] {

  private val factory = new AnyObjectFactory[R]

  private val columnMap = implicitly[ColumnMapper[R]].columnMap(tableDef)

  @transient
  private lazy val constructorColumnRefs =
    columnMap.constructor.toArray

  @transient
  private lazy val methods =
    factory.javaClass.getMethods.map(m => (m.getName, m)).toMap

  @transient
  private lazy val setters =
    columnMap.setters.toArray.map {
      case (setterName, columnRef) if !constructorColumnRefs.contains(columnRef) =>
        (methods(setterName), columnRef)
    }

  private val args = Array.ofDim[AnyRef](factory.argCount)

  private def getColumnValue(row: Row, columnRef: ColumnRef) = {
    columnRef match {
      case NamedColumnRef(name) =>
        CassandraRow.get(row, name)
      case IndexedColumnRef(index) =>
        CassandraRow.get(row, index)
    }
  }

  private def fillArgs(row: Row) {
    for (i <- 0 until args.length)
      args(i) = getColumnValue(row, constructorColumnRefs(i))
  }

  private def invokeSetters(row: Row, obj: R): R = {
    for ((setter, columnRef) <- setters)
      setter.invoke(obj, getColumnValue(row, columnRef))
    obj
  }

  override def read(row: Row, columnNames: Array[String]) = {
    fillArgs(row)
    invokeSetters(row, factory.newInstance(args: _*))
  }

  /** for testing */
  def transform(row: Array[AnyRef]) =
    factory.newInstance(row: _*)

  private def extractColumnNames(columnRefs: Iterable[ColumnRef]): Seq[String] =
    columnRefs.collect{ case NamedColumnRef(name) => name }.toSeq

  private def extractColumnIndexes(columnRefs: Iterable[ColumnRef]): Seq[Int] =
    columnRefs.collect{ case IndexedColumnRef(index) => index }.toSeq

  private val allColumnRefs = columnMap.constructor ++ columnMap.setters.values

  override def columnNames = Some(extractColumnNames(allColumnRefs))
  override def columnCount = extractColumnIndexes(allColumnRefs).reduceOption(_ max _)
}


class ClassBasedRowReaderFactory[R : TypeTag : ColumnMapper] extends RowReaderFactory[R] {
  override def rowReader(tableDef: TableDef) = new ClassBasedRowReader[R](tableDef)
}
