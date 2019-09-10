package com.datastax.spark.connector.types

import java.io.ObjectOutputStream

import com.datastax.oss.driver.api.core.`type`.{DataType, UserDefinedType => DriverUserDefinedType}
import com.datastax.oss.driver.api.core.data.{UdtValue => DriverUDTValue}
import com.datastax.spark.connector.cql.{FieldDef, StructDef}
import com.datastax.spark.connector.types.ColumnType.fromDriverType
import com.datastax.spark.connector.types.TypeAdapters.ValueByNameAdapter
import com.datastax.spark.connector.{ColumnName, UDTValue}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/** A Cassandra user defined type field metadata. It consists of a name and an associated column type.
  * The word `column` instead of `field` is used in member names because we want to treat UDT field
  * entries in the same way as table columns, so that they are mappable to case classes.
  * This is also the reason why this class extends `FieldDef`*/
case class UDTFieldDef(columnName: String, columnType: ColumnType[_]) extends FieldDef {
  override lazy val ref = ColumnName(columnName)
}

/** A Cassandra user defined type metadata.
  * A UDT consists of a sequence of ordered fields, called `columns`. */
case class UserDefinedType(
  name: String,
  columns: IndexedSeq[UDTFieldDef],
  override val isFrozen: Boolean = false)
  extends StructDef with ColumnType[UDTValue] {

  override type Column = FieldDef

  override def isMultiCell: Boolean = !isFrozen

  def isCollection = false
  def scalaTypeTag = implicitly[TypeTag[UDTValue]]
  def cqlTypeName = name

  val fieldConvereters = columnTypes.map(_.converterToCassandra)

  private lazy val valueByNameConverter = scala.util.Try(TypeConverter.forType[ValueByNameAdapter]).toOption

  override def converterToCassandra = new NullableTypeConverter[UDTValue] {
    override def targetTypeTag = UDTValue.TypeTag
    override def convertPF = {
      case udtValue: UDTValue =>
        val columnValues =
          for (i <- columns.indices) yield {
            val columnName = columnNames(i)
            val columnConverter = fieldConvereters(i)
            val columnValue = columnConverter.convert(udtValue.getRaw(columnName))
            columnValue
          }
        new UDTValue(columnNames, columnValues)
      case value if valueByNameConverter.exists(_.convertPF.isDefinedAt(value)) =>
        val valuesByName = valueByNameConverter.get.convert(value)
        val columnValues =
          for (i <- columns.indices) yield {
            val columnName = columnNames(i)
            val columnConverter = fieldConvereters(i)
            val columnValue = columnConverter.convert(valuesByName.getByName(columnName))
            columnValue
          }
        new UDTValue(columnNames, columnValues)
    }
  }

  override type ValueRepr = UDTValue

  override def newInstance(columnValues: Any*): UDTValue = {
    UDTValue(columnNames, columnValues.map(_.asInstanceOf[AnyRef]).toIndexedSeq)
  }
}

object UserDefinedType {

  /** Converts connector's UDTValue to Cassandra Java Driver UDTValue.
    * Used when saving data to Cassandra.  */
  class DriverUDTValueConverter(dataType: DriverUserDefinedType)
    extends TypeConverter[DriverUDTValue] {

    val fieldNames = dataType.getFieldNames.toIndexedSeq
    val fieldTypes = dataType.getFieldTypes.toIndexedSeq
    val fieldConverters = fieldTypes.map(ColumnType.converterToCassandra)

    override def targetTypeTag = implicitly[TypeTag[DriverUDTValue]]

    override def convertPF = {
      case udtValue: UDTValue =>
        val toSave = dataType.newValue()
        for (i <- fieldNames.indices) {
          val fieldName = fieldNames(i)
          val fieldConverter = fieldConverters(i)
          val fieldValue = fieldConverter.convert(udtValue.getRaw(fieldName.asInternal()))
          if (fieldValue == null) {
            toSave.setToNull(i)
          } else {
            toSave.set(i, fieldValue, fieldValue.getClass.asInstanceOf[Class[AnyRef]])
          }
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

  private def fields(dataType: DriverUserDefinedType): IndexedSeq[UDTFieldDef] = unlazify {
    for ((fieldName, fieldType) <- dataType.getFieldNames.zip(dataType.getFieldTypes).toIndexedSeq) yield
      UDTFieldDef(fieldName.asInternal(), fromDriverType(fieldType))
  }

  def apply(javaUserType: DriverUserDefinedType): UserDefinedType = {
    UserDefinedType(javaUserType.getName.asInternal(), fields(javaUserType), javaUserType.isFrozen)
  }
}

