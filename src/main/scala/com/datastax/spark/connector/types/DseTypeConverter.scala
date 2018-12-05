/**
  * Copyright DataStax, Inc.
  *
  * Please see the included license file for details.
  */
package com.datastax.spark.connector.types


import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.types
import org.apache.spark.unsafe.types.UTF8String
import com.datastax.driver.core.{CodecRegistry, DataType, Duration, TypeCodec}
import com.datastax.driver.dse.geometry._
import com.datastax.driver.dse.geometry.codecs._
import com.datastax.spark.connector.types.TypeConverter.OptionToNullConverter

object DseTypeConverter extends CustomDriverConverter{

  override val fromDriverRowExtension: PartialFunction[DataType, ColumnType[_]] = {
    case PointCodec.DATA_TYPE => PointType
    case PolygonCodec.DATA_TYPE => PolygonType
    case LineStringCodec.DATA_TYPE => LineStringType
  }

  override val catalystDataType: PartialFunction[ColumnType[_], types.DataType] =  {
    case LineStringType | PolygonType | PointType => types.StringType
  }

  override val catalystDataTypeConverter: PartialFunction[Any, AnyRef] =  {
    case x: Geometry => UTF8String.fromString(x.asWellKnownText())
  }

  private val PointTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Point]]
  }

  implicit object PointConverter extends NullableTypeConverter[Point] {
    def targetTypeTag = PointTypeTag
    override def convertPF = {
      case x: Point => x
      case x: String => Point.fromWellKnownText(x)
    }
  }

  private val PolygonTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Polygon]]
  }

  implicit object PolygonConverter extends NullableTypeConverter[Polygon] {
    def targetTypeTag = PolygonTypeTag
    override def convertPF = {
      case x: Polygon => x
      case x: String => Polygon.fromWellKnownText(x)
    }
  }

  private val LineStringTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[LineString]]
  }

  implicit object LineStringConverter extends NullableTypeConverter[LineString] {
    def targetTypeTag = LineStringTypeTag
    override def convertPF = {
      case x: LineString => x
      case x: String => LineString.fromWellKnownText(x)
    }
  }

  TypeConverter.registerConverter(PointConverter)
  TypeConverter.registerConverter(LineStringConverter)
  TypeConverter.registerConverter(PolygonConverter)
}

case object PointType extends PrimitiveColumnType[Point] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Point]] }
  def cqlTypeName = "PointType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[Point])
}

case object PolygonType extends PrimitiveColumnType[Polygon] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Polygon]] }
  def cqlTypeName = "PolygonType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[Polygon])
}

case object LineStringType extends PrimitiveColumnType[LineString] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[LineString]] }
  def cqlTypeName = "LineStringType"
  def converterToCassandra =
    new OptionToNullConverter(TypeConverter.forType[LineString])
}

