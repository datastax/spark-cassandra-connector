package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import scala.reflect.runtime.universe._

import TypeConverter._

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(StringConverter)
  def scalaTypeTag = implicitly[TypeTag[String]]
}

case object AsciiType extends PrimitiveColumnType[String] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(StringConverter)
  def scalaTypeTag = implicitly[TypeTag[String]]
}

case object IntType extends PrimitiveColumnType[Int] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(IntConverter)
  def scalaTypeTag = implicitly[TypeTag[Int]]
}

case object BigIntType extends PrimitiveColumnType[Long] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(LongConverter)
  def scalaTypeTag = implicitly[TypeTag[Long]]
}

case object FloatType extends PrimitiveColumnType[Float] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(FloatConverter)
  def scalaTypeTag = implicitly[TypeTag[Float]]
}

case object DoubleType extends PrimitiveColumnType[Double] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(DoubleConverter)
  def scalaTypeTag = implicitly[TypeTag[Double]]
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(BooleanConverter)
  def scalaTypeTag = implicitly[TypeTag[Boolean]]
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(JavaBigIntegerConverter)
  def scalaTypeTag = implicitly[TypeTag[BigInt]]
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(JavaBigDecimalConverter)
  def scalaTypeTag = implicitly[TypeTag[BigDecimal]]
}

case object TimestampType extends PrimitiveColumnType[Date] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(DateConverter)
  def scalaTypeTag = implicitly[TypeTag[Date]]
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(InetAddressConverter)
  def scalaTypeTag = implicitly[TypeTag[InetAddress]]
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(UUIDConverter)
  def scalaTypeTag = implicitly[TypeTag[UUID]]
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(UUIDConverter)
  def scalaTypeTag = implicitly[TypeTag[UUID]]
}

case object CounterType extends PrimitiveColumnType[Long] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(LongConverter)
  def scalaTypeTag = implicitly[TypeTag[Long]]
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  @transient
  lazy val converterToCassandra = new OptionToNullConverter(ByteBufferConverter)
  def scalaTypeTag = implicitly[TypeTag[ByteBuffer]]
}

