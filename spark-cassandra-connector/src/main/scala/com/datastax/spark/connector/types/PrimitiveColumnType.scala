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
}

case object AsciiType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
}

case object VarCharType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
}

case object IntType extends PrimitiveColumnType[Int] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Int]] }
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Long]] }
}

case object FloatType extends PrimitiveColumnType[Float] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Float]] }
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Double]] }
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Boolean]] }
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[BigInt]] }
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[BigDecimal]] }
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Date]] }
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[InetAddress]] }
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UUID]] }
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UUID]] }
}

case object CounterType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Long]] }
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[ByteBuffer]] }
}
