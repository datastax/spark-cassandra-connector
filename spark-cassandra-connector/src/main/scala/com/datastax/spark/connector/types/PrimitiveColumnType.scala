package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import TypeConverter._

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[String])
}

case object AsciiType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[String])
}

case object VarCharType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[String])
}

case object IntType extends PrimitiveColumnType[Int] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Int])
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Long])
}

case object FloatType extends PrimitiveColumnType[Float] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Float])
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Double])
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Boolean])
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[java.math.BigInteger])
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[java.math.BigDecimal])
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Date])
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[InetAddress])
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[UUID])
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[UUID])
}

case object CounterType extends PrimitiveColumnType[Long] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Long])
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[ByteBuffer])
}
