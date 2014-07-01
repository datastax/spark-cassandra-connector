package com.datastax.driver.spark.rdd.reader

import java.lang.reflect.Method

import com.datastax.driver.core.Row
import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.mapper._
import com.datastax.driver.spark.types.{TypeConversionException, TypeConverter}

import scala.reflect.runtime.universe._

/** Transforms a Cassandra Java driver `Row` into an object of a user provided class, calling the class constructor */
class ClassBasedRowReader[R : TypeTag : ColumnMapper](table: TableDef) extends RowReader[R] {

  private val factory = new AnyObjectFactory[R]

  private val columnMap = implicitly[ColumnMapper[R]].columnMap(table)

  @transient
  private val tpe = implicitly[TypeTag[R]].tpe

  @transient
  private val constructorParamTypes: Array[Type] = {
    val ctorSymbol = tpe.declaration(nme.CONSTRUCTOR).asMethod
    val ctorType = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    ctorType.params.map(_.asTerm.typeSignature).toArray
  }

  // This must be  serialized:
  val constructorArgConverters: Array[TypeConverter[_]] =
    constructorParamTypes.map(t => TypeConverter.forType(t))

  @transient
  private val setterTypes: Map[String, Type] = {
    def argType(name: String) = {
      val methodSymbol = tpe.declaration(newTermName(name)).asMethod
      methodSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType].params(0).typeSignature
    }
    columnMap.setters.keys.map(name => (name, argType(name))).toMap
  }

  val setterConverters: Map[String, TypeConverter[_]] =
    setterTypes.map { case (name, argType) => (name, TypeConverter.forType(argType)) }.toMap

  @transient
  private lazy val constructorColumnRefs =
    columnMap.constructor.toArray

  @transient
  private lazy val methods =
    factory.javaClass.getMethods.map(m => (m.getName, m)).toMap

  @transient
  private lazy val setters: Array[(Method, ColumnRef)] =
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

  private def getColumnName(row: Row, columnRef: ColumnRef) = {
    columnRef match {
      case NamedColumnRef(name) => name        
      case IndexedColumnRef(index) => row.getColumnDefinitions.getName(index)        
    }
  }

  private def convert(columnValue: AnyRef, columnName: String, converter: TypeConverter[_]): AnyRef = {
    try {
      converter.convert(columnValue).asInstanceOf[AnyRef]
    }
    catch {
      case e: Exception =>
        throw new TypeConversionException(
          s"Failed to convert column $columnName of table ${table.keyspaceName}.${table.keyspaceName} " +
          s"to ${converter.targetTypeString}: $columnValue", e)
    }
  }

  private def fillArgs(row: Row) {
    for (i <- 0 until args.length) {
      val columnRef = constructorColumnRefs(i)
      val columnName = getColumnName(row, columnRef)
      val columnValue = getColumnValue(row, columnRef)
      val converter = constructorArgConverters(i)
      args(i) = convert(columnValue, columnName, converter)
    }
  }

  private def invokeSetters(row: Row, obj: R): R = {
    for ((setter, columnRef) <- setters) {
      val columnValue = getColumnValue(row, columnRef)
      val columnName = getColumnName(row, columnRef)
      val converter = setterConverters(columnName)
      val convertedValue = convert(columnValue, columnName, converter)
      setter.invoke(obj, convertedValue)
    }
    obj
  }

  override def read(row: Row, columnNames: Array[String]) = {
    fillArgs(row)
    invokeSetters(row, factory.newInstance(args: _*))
  }

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
