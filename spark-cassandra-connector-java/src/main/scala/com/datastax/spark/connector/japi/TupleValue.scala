package com.datastax.spark.connector.japi

import scala.annotation.varargs
import scala.reflect.runtime.universe._

import com.datastax.spark.connector.{TupleValue => ConnectorTupleValue}
import com.datastax.spark.connector.types.{TypeConverter, NullableTypeConverter}

final class TupleValue private (val columnValues: IndexedSeq[AnyRef])
  extends JavaGettableByIndexData with Serializable


object TupleValue {

  val TypeTag = typeTag[TupleValue]

  implicit object UDTValueConverter extends NullableTypeConverter[TupleValue] {
    def targetTypeTag = TypeTag

    def convertPF = {
      case x: TupleValue => x
      case x: ConnectorTupleValue =>
        new TupleValue(x.columnValues)
    }
  }

  TypeConverter.registerConverter(UDTValueConverter)

  @varargs
  def newTuple(values: Object*): TupleValue =
    new TupleValue(values.toIndexedSeq)
}

