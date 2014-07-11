package com.datastax.spark.connector.types

import com.datastax.driver.core.DataType
import scala.collection.JavaConversions._

/** Serializable representation of column data type */
trait ColumnType[T] extends Serializable {

  /** Returns a converter that converts values to the type of this column expected by the
    * Cassandra Java driver when saving the row.*/
  def converterToCassandra: TypeConverter[_]

  /** Name of the Scala type. Useful for source generation.*/
  def scalaTypeName: String

  def isCollection: Boolean
}

object ColumnType {

  private val primitiveTypeMap = Map[DataType, ColumnType[_]](
    DataType.text() -> TextType,
    DataType.ascii() -> AsciiType,
    DataType.cint() -> IntType,
    DataType.bigint() -> BigIntType,
    DataType.cfloat() -> FloatType,
    DataType.cdouble() -> DoubleType,
    DataType.cboolean() -> BooleanType,
    DataType.varint() -> VarIntType,
    DataType.decimal() -> DecimalType,
    DataType.timestamp() -> TimestampType,
    DataType.inet() -> InetType,
    DataType.uuid() -> UUIDType,
    DataType.blob() -> BlobType,
    DataType.counter() -> CounterType,
    DataType.timeuuid() -> TimeUUIDType
  )

  def fromDriverType(dataType: DataType): ColumnType[_] = {
    val typeArgs = dataType.getTypeArguments.map(fromDriverType)
    dataType.getName match {
      case DataType.Name.LIST => ListType(typeArgs(0))
      case DataType.Name.SET => SetType(typeArgs(0))
      case DataType.Name.MAP => MapType(typeArgs(0), typeArgs(1))
      case _ => primitiveTypeMap(dataType)
    }
  }
}
