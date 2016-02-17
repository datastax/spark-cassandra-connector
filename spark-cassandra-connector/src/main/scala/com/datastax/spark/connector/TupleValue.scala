package com.datastax.spark.connector

import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.ReflectionLock.SparkReflectionLock

import com.datastax.driver.core.{TupleValue => DriverTupleValue}
import com.datastax.spark.connector.types.NullableTypeConverter

final case class TupleValue(values: Any*) extends ScalaGettableByIndexData {
  override def columnValues = values.toIndexedSeq.map(_.asInstanceOf[AnyRef])
}

object TupleValue {

  def fromJavaDriverTupleValue
      (value: DriverTupleValue)
      : TupleValue = {
    val values =
      for (i <- 0 until value.getType.getComponentTypes.size()) yield
        GettableData.get(value, i)
    new TupleValue(values: _*)
  }

  val TypeTag = SparkReflectionLock.synchronized(typeTag[TupleValue])
  val Symbol = SparkReflectionLock.synchronized(typeOf[TupleValue].asInstanceOf[TypeRef].sym)

  implicit object TupleValueConverter extends NullableTypeConverter[TupleValue] {
    def targetTypeTag = TypeTag
    def convertPF = {
      case x: TupleValue => x
    }
  }
}