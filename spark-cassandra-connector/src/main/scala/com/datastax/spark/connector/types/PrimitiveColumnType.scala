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
}

case object AsciiType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
  def cqlTypeName = "ascii"
}

case object VarCharType extends PrimitiveColumnType[String] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[String]] }
  def cqlTypeName = "varchar"
}

case object IntType extends PrimitiveColumnType[Int] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Int]] }
  def cqlTypeName = "int"
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Long]] }
  def cqlTypeName = "bigint"
}

case object FloatType extends PrimitiveColumnType[Float] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Float]] }
  def cqlTypeName = "float"
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Double]] }
  def cqlTypeName = "double"
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Boolean]] }
  def cqlTypeName = "boolean"
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[BigInt]] }
  def cqlTypeName = "varint"
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[BigDecimal]] }
  def cqlTypeName = "decimal"
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Date]] }
  def cqlTypeName = "timestamp"
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[InetAddress]] }
  def cqlTypeName = "inet"
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UUID]] }
  def cqlTypeName = "uuid"
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UUID]] }
  def cqlTypeName = "timeuuid"
}

case object CounterType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[Long]] }
  def cqlTypeName = "counter"
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[ByteBuffer]] }
  def cqlTypeName = "blob"
}
