package com.datastax.spark.connector

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import org.apache.commons.lang3.tuple

import com.datastax.driver.core.{ProtocolVersion, UDTValue => DriverUDTValue}
import com.datastax.spark.connector.types.NullableTypeConverter

final case class UDTValue(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData {
  override def productArity: Int = columnValues.size
  override def productElement(i: Int) = columnValues(i)
}

object UDTValue {

  def fromJavaDriverUDTValue(value: DriverUDTValue)(implicit protocolVersion: ProtocolVersion): UDTValue = {
    val fields = value.getType.getFieldNames.toIndexedSeq
    val values = fields.map(GettableData.get(value, _))
    UDTValue(fields, values)
  }

  def fromMap(map: Map[String, Any]): UDTValue =
    new UDTValue(map.keys.toIndexedSeq, map.values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)

  val TypeTag = implicitly[TypeTag[UDTValue]]
  val Symbol = typeOf[UDTValue].asInstanceOf[TypeRef].sym

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = TypeTag
    def convertPF = {
      case x: UDTValue => x
    }
  }
}
