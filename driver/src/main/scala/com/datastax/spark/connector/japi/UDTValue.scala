package com.datastax.spark.connector.japi

import com.datastax.spark.connector.types.{NullableTypeConverter, TypeConverter}
import com.datastax.spark.connector.{CassandraRowMetadata, UDTValue => ConnectorUDTValue}

import scala.reflect.runtime.universe._

final class UDTValue(val metaData: CassandraRowMetadata, val columnValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable

object UDTValue {

  val UDTValueTypeTag = implicitly[TypeTag[UDTValue]]

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = UDTValueTypeTag

    def convertPF = {
      case x: UDTValue => x
      case x: ConnectorUDTValue =>
        new UDTValue(x.metaData, x.columnValues)
    }
  }

  TypeConverter.registerConverter(UDTValueConverter)

}