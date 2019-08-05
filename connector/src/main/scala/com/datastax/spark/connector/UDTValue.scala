package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.data.{UdtValue => DriverUDTValue}
import com.datastax.spark.connector.types.NullableTypeConverter
import com.datastax.spark.connector.util.DriverUtil.toName

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

final case class UDTValue(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData {
  override def productArity: Int = columnValues.size
  override def productElement(i: Int) = columnValues(i)

  override def metaData = CassandraRowMetadata.fromColumnNames(columnNames)
}

object UDTValue {

  def fromJavaDriverUDTValue(value: DriverUDTValue): UDTValue = {
    val fields = value.getType.getFieldNames.map(f => toName(f)).toIndexedSeq
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
