package com.datastax.spark.connector.types


import scala.reflect.runtime.universe.TypeTag

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import com.datastax.driver.core.LocalDate

trait PrimitiveColumnType[T] extends ColumnType[T] {
  def isCollection = false
}

case object TextType extends PrimitiveColumnType[String] {
  def scalaTypeTag = implicitly[TypeTag[String]]
  def cqlTypeName = "text"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object AsciiType extends PrimitiveColumnType[String] {
  def scalaTypeTag = implicitly[TypeTag[String]]
  def cqlTypeName = "ascii"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object VarCharType extends PrimitiveColumnType[String] {
  def scalaTypeTag = implicitly[TypeTag[String]]
  def cqlTypeName = "varchar"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[String])
}

case object IntType extends PrimitiveColumnType[Int] {
  def scalaTypeTag = implicitly[TypeTag[Int]]
  def cqlTypeName = "int"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Integer])
}

case object BigIntType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = implicitly[TypeTag[Long]]
  def cqlTypeName = "bigint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Long])
}

case object SmallIntType extends PrimitiveColumnType[Short] {
  def scalaTypeTag = implicitly[TypeTag[Short]]
  def cqlTypeName = "smallint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Short])
}

case object TinyIntType extends PrimitiveColumnType[Byte] {
  def scalaTypeTag = implicitly[TypeTag[Byte]]
  def cqlTypeName = "tinyint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Byte])
}

case object FloatType extends PrimitiveColumnType[Float] {
  def scalaTypeTag = implicitly[TypeTag[Float]]
  def cqlTypeName = "float"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Float])
}

case object DoubleType extends PrimitiveColumnType[Double] {
  def scalaTypeTag = implicitly[TypeTag[Double]]
  def cqlTypeName = "double"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Double])
}

case object BooleanType extends PrimitiveColumnType[Boolean] {
  def scalaTypeTag = implicitly[TypeTag[Boolean]]
  def cqlTypeName = "boolean"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Boolean])
}

case object VarIntType extends PrimitiveColumnType[BigInt] {
  def scalaTypeTag = implicitly[TypeTag[BigInt]]
  def cqlTypeName = "varint"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.math.BigInteger])
}

case object DecimalType extends PrimitiveColumnType[BigDecimal] {
  def scalaTypeTag = implicitly[TypeTag[BigDecimal]]
  def cqlTypeName = "decimal"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.math.BigDecimal])
}

case object TimestampType extends PrimitiveColumnType[Date] {
  def scalaTypeTag = implicitly[TypeTag[Date]]
  def cqlTypeName = "timestamp"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.util.Date])
}

case object InetType extends PrimitiveColumnType[InetAddress] {
  def scalaTypeTag = implicitly[TypeTag[InetAddress]]
  def cqlTypeName = "inet"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[InetAddress])
}

case object UUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "uuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[UUID])
}

case object TimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "timeuuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[UUID])
}

case object CounterType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = implicitly[TypeTag[Long]]
  def cqlTypeName = "counter"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.lang.Long])
}

case object BlobType extends PrimitiveColumnType[ByteBuffer] {
  def scalaTypeTag = implicitly[TypeTag[ByteBuffer]]
  def cqlTypeName = "blob"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[java.nio.ByteBuffer])
}

case object DateType extends PrimitiveColumnType[Int] {
  def scalaTypeTag = implicitly[TypeTag[Int]]
  def cqlTypeName = "date"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.forType[LocalDate])
}

/* The implicit conversions we usually do between types will not work as expected here, Since the
time representation is stored as nanoseconds from midnight (epoch) we need to make sure any
translations of Types take this into account. In particular we need to be careful of reading this
time out and converting it to Dates since they will be off by a factor of a million.
 */
case object TimeType extends PrimitiveColumnType[Long] {
  def scalaTypeTag = implicitly[TypeTag[Long]]
  def cqlTypeName = "time"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(TypeConverter.TimeTypeConverter)
}
