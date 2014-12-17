package com.datastax.spark.connector.types

import java.io.ObjectOutputStream

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import com.datastax.driver.core.{UDTValue => DriverUDTValue, ProtocolVersion, UserType, DataType}
import com.datastax.spark.connector.UDTValue
import com.datastax.spark.connector.types.TypeConverter.OptionToNullConverter

case class FieldDef(fieldName: String, fieldType: ColumnType[_])

case class UserDefinedType(fields: Seq[FieldDef]) extends ColumnType[UDTValue] {
  lazy val fieldNames = fields.toIndexedSeq.map(_.fieldName)
  lazy val fieldTypes = fields.toIndexedSeq.map(_.fieldType)
  override def isCollection = false
  override def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UDTValue]] }
}


object UserDefinedType {

  class DriverUDTValueConverter(dataType: UserType)(implicit protocolVersion: ProtocolVersion)
    extends TypeConverter[DriverUDTValue] {

    val fieldNames = dataType.getFieldNames.toIndexedSeq
    val fieldTypes = fieldNames.map(dataType.getFieldType)
    val fieldConverters = fieldTypes.map(ColumnType.converterToCassandra)

    override def targetTypeTag = TypeTag.synchronized { implicitly[TypeTag[DriverUDTValue]] }

    override def convertPF = {
      case udtValue: UDTValue =>
        val toSave = dataType.newValue()
        for (i <- 0 until fieldNames.size) {
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

