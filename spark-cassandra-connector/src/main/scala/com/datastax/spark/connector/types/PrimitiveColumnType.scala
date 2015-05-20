package com.datastax.spark.connector.types


import scala.reflect.runtime.universe.TypeTag

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
  def cqlTypeName = "text"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object AsciiType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
  def cqlTypeName = "ascii"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object VarCharType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
  def cqlTypeName = "varchar"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object IntType extends PrimitiveColumnType[Int] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Int]] }
  def cqlTypeName = "int"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Integer])
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Long]] }
  def cqlTypeName = "bigint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Long])
}

case object FloatType extends PrimitiveColumnType[Float] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Float]] }
  def cqlTypeName = "float"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Float])
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Double]] }
  def cqlTypeName = "double"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Double])
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Boolean]] }
  def cqlTypeName = "boolean"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Boolean])
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[BigInt]] }
  def cqlTypeName = "varint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.math.BigInteger])
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[BigDecimal]] }
  def cqlTypeName = "decimal"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.math.BigDecimal])
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Date]] }
  def cqlTypeName = "timestamp"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.util.Date])
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[InetAddress]] }
  def cqlTypeName = "inet"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[InetAddress])
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UUID]] }
  def cqlTypeName = "uuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[UUID])
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UUID]] }
  def cqlTypeName = "timeuuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[UUID])
}

case object CounterType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Long]] }
  def cqlTypeName = "counter"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Long])
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[ByteBuffer]] }
  def cqlTypeName = "blob"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.nio.ByteBuffer])
}
