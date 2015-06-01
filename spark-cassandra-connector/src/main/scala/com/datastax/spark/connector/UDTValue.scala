package com.datastax.spark.connector

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import com.datastax.driver.core.{ProtocolVersion, UDTValue => DriverUDTValue}
import com.datastax.spark.connector.types.NullableTypeConverter

final class UDTValue(val columnNames: IndexedSeq[String], val columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData with Serializable {

}

object UDTValue {

  def fromJavaDriverUDTValue(value: DriverUDTValue)(implicit protocolVersion: ProtocolVersion): UDTValue = {
    val fields = value.getType.getFieldNames.toIndexedSeq
    val values = fields.map(GettableData.get(value, _))
    new UDTValue(fields, values)
  }

  def fromMap(map: Map[String, Any]): UDTValue =
    new UDTValue(map.keys.toIndexedSeq, map.values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)

  val UDTValueTypeTag = implicitly[TypeTag[UDTValue]]

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = UDTValueTypeTag
    def convertPF = {
      case x: UDTValue => x
    }
  }
}
