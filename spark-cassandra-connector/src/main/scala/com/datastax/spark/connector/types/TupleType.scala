package com.datastax.spark.connector.types


import java.io.ObjectOutputStream

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import org.apache.commons.lang3.tuple.{Triple, Pair}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import com.datastax.driver.core.{TupleValue => DriverTupleValue, TupleType => DriverTupleType, DataType}
import com.datastax.spark.connector.{TupleValue, ColumnName}
import com.datastax.spark.connector.cql.{FieldDef, StructDef}
import com.datastax.spark.connector.util.CodecRegistryUtil

case class TupleFieldDef(index: Int, columnType: ColumnType[_]) extends FieldDef {
  override def columnName = index.toString
  override lazy val ref = ColumnName(columnName)
}

/** A type representing typed tuples.
  * A tuple consists of a sequence of values.
  * Every value is identified by its 0-based position.
  * Every value can be of a different type. */
case class TupleType(componentTypes: TupleFieldDef*)
    extends StructDef with ColumnType[TupleValue] {

  override type ValueRepr = TupleValue

  override type Column = TupleFieldDef

  for ((c, i) <- componentTypes.zipWithIndex) {
    if (c.index != i)
      throw new IllegalArgumentException(s"Invalid tuple component index: ${c.index}. Expected: $i")
  }

  override val columns = componentTypes.toIndexedSeq

  override def scalaTypeTag = TupleValue.TypeTag

  override def isCollection = false

  private val defaultComponentConverters =
    componentTypes.map(_.columnType.converterToCassandra).toIndexedSeq

  /** Creates new tuple from components converted each to the
    * type determined by an appropriate componentType.
    * Throws IllegalArgumentException if the number of components does
    * not match the number of components in the tuple type. */
  def newInstance
      (componentConverters: IndexedSeq[TypeConverter[_ <: AnyRef]])
      (componentValues: Any*): TupleValue = {
    require(
      componentValues.length == columns.length,
      s"Expected ${columns.length} components, instead of ${componentValues.length}")
    val values =
      for (i <- columns.indices) yield
        componentConverters(i).convert(componentValues(i))
    new TupleValue(values: _*)
  }

  override def newInstance(componentValues: Any*): TupleValue =
    newInstance(defaultComponentConverters)(componentValues: _*)

  def converterToCassandra(componentConverters: IndexedSeq[TypeConverter[_ <: AnyRef]]) = {
    new TypeConverter[TupleValue] {

      override def targetTypeTag = TupleValue.TypeTag

      override def convertPF = {
        case x: GenericRowWithSchema =>
          newInstance(componentConverters)(x.toSeq: _*)
        case x: TupleValue =>
          newInstance(componentConverters)(x.columnValues: _*)
        case x: Product => // converts from Scala tuples
          newInstance(componentConverters)(x.productIterator.toIndexedSeq: _*)
        case x: Pair[_, _] => // Java programmers may like this
          newInstance(componentConverters)(x.getLeft, x.getRight)
        case x: Triple[_, _, _] => // Java programmers may like this
          newInstance(componentConverters)(x.getLeft, x.getMiddle, x.getRight)
      }
    }
  }

  override def converterToCassandra: TypeConverter[TupleValue] =
    converterToCassandra(defaultComponentConverters)

  override def cqlTypeName = {
    val types = columnTypes.map(_.cqlTypeName)
    s"frozen<tuple<${types.mkString(", ")}>>"
  }

  override val name = cqlTypeName

}

object TupleType {

  /** Converts connector's UDTValue to Cassandra Java Driver UDTValue.
    * Used when saving data to Cassandra.  */
  class DriverTupleValueConverter(dataType: DriverTupleType)
    extends TypeConverter[DriverTupleValue] {

    val fieldTypes = dataType.getComponentTypes
    val fieldConverters = fieldTypes.map(ColumnType.converterToCassandra)

    override def targetTypeTag = typeTag[DriverTupleValue]

    override def convertPF = {
      case tupleValue: TupleValue =>
        val toSave = dataType.newValue()
        for (i <- 0 until fieldTypes.size) {
          val fieldConverter = fieldConverters(i)
          val fieldValue = fieldConverter.convert(tupleValue.getRaw(i))
          toSave.set(i, fieldValue,  CodecRegistryUtil.codecFor(fieldTypes(i), fieldValue))
        }
        toSave
    }

    // Fortunately we ain't gonna need serialization, because this TypeConverter is used only on the
    // write side and instantiated separately on each executor node.
    private def writeObject(oos: ObjectOutputStream): Unit =
      throw new UnsupportedOperationException(
        this.getClass.getName + " does not support serialization, because the " +
          "required underlying " + classOf[DataType].getName + " is not Serializable.")

  }

  def driverTupleValueConverter(dataType: DataType): TypeConverter[_] = {
    dataType match {
      case dt: DriverTupleType => new DriverTupleValueConverter(dt)
      case _ => throw new IllegalArgumentException(s"${classOf[DriverTupleType]} expected.")
    }
  }
}



