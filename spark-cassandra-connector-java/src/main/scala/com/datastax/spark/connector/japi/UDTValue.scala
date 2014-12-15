package com.datastax.spark.connector.japi

import scala.reflect.runtime.universe._

import com.datastax.spark.connector.types.{TypeConverter, NullableTypeConverter}
import com.datastax.spark.connector.{UDTValue => ConnectorUDTValue}

final class UDTValue(val fieldNames: IndexedSeq[String], val fieldValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable

object UDTValue {

  val UDTValueTypeTag = implicitly[TypeTag[UDTValue]]

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = UDTValueTypeTag

    def convertPF = {
      case x: UDTValue => x
      case x: ConnectorUDTValue =>
        new UDTValue(x.fieldNames, x.fieldValues)
    }
  }

  TypeConverter.registerConverter(UDTValueConverter)

}