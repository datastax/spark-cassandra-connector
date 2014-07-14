package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import TypeConverter._

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(StringConverter)
  def scalaTypeName = "String"
}

case object AsciiType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(StringConverter)
  def scalaTypeName = "String"
}

case object IntType extends PrimitiveColumnType[Int] {
  def converterToCassandra = new OptionToNullConverter(IntConverter)
  def scalaTypeName = "Int"
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def converterToCassandra = new OptionToNullConverter(LongConverter)
  def scalaTypeName = "Long"
}

case object FloatType extends PrimitiveColumnType[Float] {
  def converterToCassandra = new OptionToNullConverter(FloatConverter)
  def scalaTypeName = "Float"
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def converterToCassandra = new OptionToNullConverter(DoubleConverter)
  def scalaTypeName = "Double"
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def converterToCassandra = new OptionToNullConverter(BooleanConverter)
  def scalaTypeName = "Boolean"
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def converterToCassandra = new OptionToNullConverter(JavaBigIntegerConverter)
  def scalaTypeName = "BigInt"
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def converterToCassandra = new OptionToNullConverter(JavaBigDecimalConverter)
  def scalaTypeName = "BigDecimal"
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def converterToCassandra = new OptionToNullConverter(DateConverter)
  def scalaTypeName = "java.util.Date"
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def converterToCassandra = new OptionToNullConverter(InetAddressConverter)
  def scalaTypeName = "java.net.InetAddress"
}
case object UUIDType extends PrimitiveColumnType[UUID] {
  def converterToCassandra = new OptionToNullConverter(UUIDConverter)
  def scalaTypeName = "java.util.UUID"
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def converterToCassandra = new OptionToNullConverter(UUIDConverter)
  def scalaTypeName = "java.util.UUID"
}

case object CounterType extends PrimitiveColumnType[Long] {
  def converterToCassandra = new OptionToNullConverter(LongConverter)
  def scalaTypeName = "Long"
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def converterToCassandra = new OptionToNullConverter(ByteBufferConverter)
  def scalaTypeName = "java.nio.ByteBuffer"
}

