package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{ UUID, Date }

import scala.reflect.runtime.universe._

import TypeConverter._

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[String])
  def scalaTypeTag = implicitly[TypeTag[String]]
}

case object AsciiType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[String])
  def scalaTypeTag = implicitly[TypeTag[String]]
}

case object VarCharType extends PrimitiveColumnType[String] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[String])
  def scalaTypeTag = implicitly[TypeTag[String]]
}

case object IntType extends PrimitiveColumnType[Int] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Int])
  def scalaTypeTag = implicitly[TypeTag[Int]]
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Long])
  def scalaTypeTag = implicitly[TypeTag[Long]]
}

case object FloatType extends PrimitiveColumnType[Float] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Float])
  def scalaTypeTag = implicitly[TypeTag[Float]]
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Double])
  def scalaTypeTag = implicitly[TypeTag[Double]]
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Boolean])
  def scalaTypeTag = implicitly[TypeTag[Boolean]]
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[java.math.BigInteger])
  def scalaTypeTag = implicitly[TypeTag[BigInt]]
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[java.math.BigDecimal])
  def scalaTypeTag = implicitly[TypeTag[BigDecimal]]
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Date])
  def scalaTypeTag = implicitly[TypeTag[Date]]
}

case object InetType extends PrimitiveColumnType[InetAddress] {

  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[InetAddress])
  def scalaTypeTag = implicitly[TypeTag[InetAddress]]
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[UUID])
  def scalaTypeTag = implicitly[TypeTag[UUID]]
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[UUID])
  def scalaTypeTag = implicitly[TypeTag[UUID]]
}

case object CounterType extends PrimitiveColumnType[Long] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[Long])
  def scalaTypeTag = implicitly[TypeTag[Long]]
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[ByteBuffer])
  def scalaTypeTag = implicitly[TypeTag[ByteBuffer]]
}

