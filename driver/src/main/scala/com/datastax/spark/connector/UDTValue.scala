package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.data.{UdtValue => DriverUDTValue}
import com.datastax.spark.connector.types.NullableTypeConverter
import com.datastax.spark.connector.util.DriverUtil.toName

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

final case class UDTValue(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData {

  private var metaData_ : Option[CassandraRowMetadata] = None

  lazy val metaData : CassandraRowMetadata = metaData_ match {
    case Some(x) => x
    case None => CassandraRowMetadata.fromColumnNames(columnNames)
  }

  def this(metaData: CassandraRowMetadata, columnValues: IndexedSeq[AnyRef]) = {
    this(metaData.columnNames, columnValues)
    this.metaData_ = Some(metaData)
  }

  override def productArity: Int = columnValues.size
  override def productElement(i: Int) = columnValues(i)
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

  def apply(metaData: CassandraRowMetadata, columnValues: IndexedSeq[AnyRef]): UDTValue =
    new UDTValue(metaData, columnValues)
}
