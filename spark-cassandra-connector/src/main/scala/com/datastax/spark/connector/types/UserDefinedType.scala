package com.datastax.spark.connector.types

import java.io.ObjectOutputStream

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import com.datastax.driver.core.{UDTValue => DriverUDTValue, ProtocolVersion, UserType, DataType}
import com.datastax.spark.connector.{ColumnName, UDTValue}
import com.datastax.spark.connector.cql.{StructDef, FieldDef}

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
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UDTValue]] }
  def cqlTypeName = name

  def converterToCassandra = new TypeConverter[UDTValue] {
    override def targetTypeTag = UDTValue.UDTValueTypeTag
    override def convertPF = {
      case udtValue: UDTValue =>
        val columnValues =
          for (i <- columns.indices) yield {
            val columnName = columnNames(i)
            val columnConverter = columnTypes(i).converterToCassandra
            val columnValue = columnConverter.convert(udtValue.getRaw(columnName))
            columnValue
          }
        new UDTValue(columnNames, columnValues)
    }
  }
}

object UserDefinedType {

  /** Converts connector's UDTValue to Cassandra Java Driver UDTValue.
    * Used when saving data to Cassandra.  */
  class DriverUDTValueConverter(dataType: UserType)(implicit protocolVersion: ProtocolVersion)
    extends TypeConverter[DriverUDTValue] {

    val fieldNames = dataType.getFieldNames.toIndexedSeq
    val fieldTypes = fieldNames.map(dataType.getFieldType)
    val fieldConverters = fieldTypes.map(ColumnType.converterToCassandra)

    override def targetTypeTag = TypeTag.synchronized { implicitly[TypeTag[DriverUDTValue]] }

    override def convertPF = {
      case udtValue: UDTValue =>
        val toSave = dataType.newValue()
        for (i <- fieldNames.indices) {
          val fieldName = fieldNames(i)
          val fieldConverter = fieldConverters(i)
          val fieldValue = fieldConverter.convert(udtValue.getRaw(fieldName))
          val fieldType = fieldTypes(i)
          val serialized =
            if (fieldValue != null) fieldType.serialize(fieldValue, protocolVersion)
            else null
          toSave.setBytesUnsafe(i, serialized)
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

  def driverUDTValueConverter(dataType: DataType)(implicit protocolVersion: ProtocolVersion) =
    dataType match {
      case dt: UserType => new DriverUDTValueConverter(dt)
      case _            => throw new IllegalArgumentException("UserType expected.")
    }

}

