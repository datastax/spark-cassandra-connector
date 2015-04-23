package com.datastax.spark.connector.rdd.reader

import java.lang.reflect.Method

import com.datastax.driver.core.{ProtocolVersion, Row}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.types.{TypeConversionException, TypeConverter}
import com.datastax.spark.connector.util.JavaApiHelper
import com.datastax.spark.connector.util.Reflect

import scala.reflect.runtime.universe._

/** Transforms a Cassandra Java driver `Row` into an object of a user provided class, calling the class constructor */
class ClassBasedRowReader[R: TypeTag : ColumnMapper](table: TableDef, skipColumns: Int = 0, aliasToColumnName: Map[String, String] = Map.empty)
  extends RowReader[R] {

  private[connector] val factory = new AnyObjectFactory[R]

  private val columnMap = implicitly[ColumnMapper[R]].columnMap(table, aliasToColumnName)

  @transient
  private val tpe = TypeTag.synchronized(implicitly[TypeTag[R]].tpe)

  // This must be  serialized:
  val constructorArgConverters: Array[TypeConverter[_]] =
    factory.constructorParamTypes.map(t => TypeConverter.forType(t))

  @transient
  private val setterTypes: Map[String, Type] = {
    def argType(name: String) = {
      val symbol = Reflect.member(tpe, name)
      if (symbol.isMethod)
        symbol.asMethod.typeSignatureIn(tpe).asInstanceOf[MethodType].params(0).typeSignature
      else
        throw new IllegalArgumentException(s"The provided type $tpe does not implement the method $name")
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
    columnMap.setters.toArray.collect {
      case (setterName, columnRef) if !constructorColumnRefs.contains(columnRef) =>
        (methods(setterName), columnRef)
    }

  @transient
  private lazy val buffer = new ThreadLocal[Array[AnyRef]] {
    override def initialValue() = Array.ofDim[AnyRef](factory.argCount)
  }

  private def getColumnValue(row: Row, columnRef: ColumnRef)(implicit protocolVersion: ProtocolVersion) = {
    columnRef match {
      case SelectableColumnRef(selectedAs) =>
        GettableData.get(row, selectedAs)
      case ColumnIndex(index) =>
        GettableData.get(row, index + skipColumns)
    }
  }

  private def getColumnName(row: Row, columnRef: ColumnRef) = {
    columnRef match {
      case SelectableColumnRef(selectedAs) => selectedAs
      case ColumnIndex(index) => row.getColumnDefinitions.getName(index + skipColumns)
    }
  }

  private def convert(columnValue: AnyRef, columnName: String, converter: TypeConverter[_]): AnyRef = {
    try {
      converter.convert(columnValue).asInstanceOf[AnyRef]
    }
    catch {
      case e: Exception =>
        throw new TypeConversionException(
          s"Failed to convert column $columnName of table ${table.keyspaceName}.${table.tableName} " +
            s"to ${converter.targetTypeName}: $columnValue", e)
    }
  }

  private def fillBuffer(row: Row, buf: Array[AnyRef])(implicit protocolVersion: ProtocolVersion) {
    for (i <- 0 until buf.length) {
      val columnRef = constructorColumnRefs(i)
      val columnName = getColumnName(row, columnRef)
      val columnValue = getColumnValue(row, columnRef)
      val converter = constructorArgConverters(i)
      buf(i) = convert(columnValue, columnName, converter)
    }
  }

  private def invokeSetters(row: Row, obj: R)(implicit protocolVersion: ProtocolVersion): R = {
    for ((setter, columnRef) <- setters) {
      val columnValue = getColumnValue(row, columnRef)
      val columnName = getColumnName(row, columnRef)
      val converter = setterConverters(setter.getName)
      val convertedValue = convert(columnValue, columnName, converter)
      if (!columnMap.allowsNull && convertedValue == null) {
        throw new NullPointerException(
          "Unexpected null value of column " + columnName + ". " +
            "If you want to receive null values from Cassandra, please wrap the column type into Option " +
            "or use JavaBeanColumnMapper")
      }
      setter.invoke(obj, convertedValue)
    }
    obj
  }

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion) = {
    val buf = buffer.get
    fillBuffer(row, buf)
    invokeSetters(row, factory.newInstance(buf: _*))
  }

  private def extractColumnNames(columnRefs: Iterable[ColumnRef]): Seq[String] =
    columnRefs.collect{ case ColumnName(name, _) => name }.toSeq

  private def extractColumnIndexes(columnRefs: Iterable[ColumnRef]): Seq[Int] =
    columnRefs.collect{ case ColumnIndex(index) => index }.toSeq

  private val allColumnRefs = columnMap.constructor ++ columnMap.setters.values

  override def columnNames = Some(extractColumnNames(allColumnRefs))
  override def requiredColumns = extractColumnIndexes(allColumnRefs).reduceOption(_ max _)
  override def consumedColumns: Option[Int] = {
    val keyIsTuple = tpe.typeSymbol.fullName startsWith "scala.Tuple"
    if (keyIsTuple) Some(factory.argCount) else None
  }
}


class ClassBasedRowReaderFactory[R : TypeTag : ColumnMapper] extends RowReaderFactory[R] {
  override def rowReader(tableDef: TableDef, options: RowReaderOptions) =
    new ClassBasedRowReader[R](tableDef, options.offset, options.aliasToColumnName)

  override def targetClass: Class[R] = JavaApiHelper.getRuntimeClass(typeTag[R])
}
