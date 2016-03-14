package com.datastax.spark.connector.japi

import scala.reflect.runtime.universe._

import com.datastax.spark.connector.types.{TypeConverter, NullableTypeConverter}
import com.datastax.spark.connector.{UDTValue => ConnectorUDTValue}

final class UDTValue(val columnNames: IndexedSeq[String], val columnValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable

object UDTValue {

  val UDTValueTypeTag = implicitly[TypeTag[UDTValue]]

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = UDTValueTypeTag

    def convertPF = {
      case x: UDTValue => x
      case x: ConnectorUDTValue =>
        new UDTValue(x.columnNames, x.columnValues)
    }
  }

  TypeConverter.registerConverter(UDTValueConverter)

}