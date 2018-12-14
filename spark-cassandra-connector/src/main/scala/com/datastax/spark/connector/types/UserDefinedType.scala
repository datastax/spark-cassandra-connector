package com.datastax.spark.connector.types

import java.io.ObjectOutputStream

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import com.datastax.driver.core.{UDTValue => DriverUDTValue, UserType, DataType}
import com.datastax.spark.connector.{ColumnName, UDTValue}
import com.datastax.spark.connector.cql.{StructDef, FieldDef}
import com.datastax.spark.connector.util.CodecRegistryUtil

/** A Cassandra user defined type field metadata. It consists of a name and an associated column type.
  * The word `column` instead of `field` is used in member names because we want to treat UDT field
  * entries in the same way as table columns, so that they are mappable to case classes.
  * This is also the reason why this class extends `FieldDef`*/
case class UDTFieldDef(columnName: String, columnType: ColumnType[_]) extends FieldDef {
  override lazy val ref = ColumnName(columnName)
}

/** A Cassandra user defined type metadata.
  * A UDT consists of a sequence of ordered fields, called `columns`. */
case class UserDefinedType(name: String, columns: IndexedSeq[UDTFieldDef])
  extends StructDef with ColumnType[UDTValue] {

  override type Column = FieldDef

  def isCollection = false
  def scalaTypeTag = implicitly[TypeTag[UDTValue]]
  def cqlTypeName = name

  val fieldConverters = columnTypes.map(_.converterToCassandra)

  def converterToCassandra = new NullableTypeConverter[UDTValue] {
    override def targetTypeTag = UDTValue.TypeTag
    override def convertPF = {
      case udtValue: UDTValue =>
        val columnValues =
          for (i <- columns.indices) yield {
            val columnName = columnNames(i)
            val columnConverter = fieldConverters(i)
            val columnValue = columnConverter.convert(udtValue.getRaw(columnName))
            columnValue
          }
        new UDTValue(columnNames, columnValues)
      case dfGenericRow: GenericRowWithSchema =>
        val columnValues =
         for (i <- columns.indices) yield {
           val columnName = columnNames(i)
           val columnConverter = fieldConverters(i)
           val dfSchemaIndex = dfGenericRow.schema.fieldIndex(columnName)
           val columnValue = columnConverter.convert(dfGenericRow.get(dfSchemaIndex))
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
  class DriverUDTValueConverter(dataType: UserType)
    extends TypeConverter[DriverUDTValue] {

    val fieldNames = dataType.getFieldNames.toIndexedSeq
    val fieldTypes = fieldNames.map(dataType.getFieldType)
    val fieldConverters = fieldTypes.map(ColumnType.converterToCassandra)

    override def targetTypeTag = implicitly[TypeTag[DriverUDTValue]]

    override def convertPF = {
      case udtValue: UDTValue =>
        val toSave = dataType.newValue()
        for (i <- fieldNames.indices) {
          val fieldName = fieldNames(i)
          val fieldConverter = fieldConverters(i)
          val fieldValue = fieldConverter.convert(udtValue.getRaw(fieldName))
          toSave.set(i, fieldValue, CodecRegistryUtil.codecFor(fieldTypes(i), fieldValue))
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

  def driverUDTValueConverter(dataType: DataType) =
    dataType match {
      case dt: UserType => new DriverUDTValueConverter(dt)
      case _            => throw new IllegalArgumentException("UserType expected.")
    }

}

