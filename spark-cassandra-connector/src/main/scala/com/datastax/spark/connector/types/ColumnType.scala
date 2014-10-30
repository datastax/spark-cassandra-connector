package com.datastax.spark.connector.types

import com.datastax.driver.core.{UDTValue, DataType}
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import com.datastax.spark.connector.types.TypeConverter.OptionToNullConverter

/** Serializable representation of column data type. */
trait ColumnType[T] extends Serializable {

  /** Returns a converter that converts values to the type of this column expected by the
    * Cassandra Java driver when saving the row.*/
  def converterToCassandra: TypeConverter[_]

  /** Returns a converter that converts values to the Scala type associated with this column. */
  lazy val converterToScala: TypeConverter[T] =
    TypeConverter.forType(scalaTypeTag)

  /** Returns the TypeTag of the Scala type recommended to represent values of this column. */
  def scalaTypeTag: TypeTag[T]

  /** Name of the Scala type. Useful for source generation.*/
  def scalaTypeName: String
    = scalaTypeTag.tpe.toString

  def isCollection: Boolean
}

object ColumnType {

  private val primitiveTypeMap = Map[DataType, ColumnType[_]](
    DataType.text() -> TextType,
    DataType.ascii() -> AsciiType,
    DataType.varchar() -> VarCharType,
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
      case DataType.Name.UDT => UserDefinedTypeStub
      case _ => primitiveTypeMap(dataType)
    }
  }
}

// TODO: This is a stub.
// UDTValues are not Serializable.
// Properly, we should use a dedicated,
// serializable class for UDTValues and also allow to map them to case classes.
case object UserDefinedTypeStub extends ColumnType[UDTValue] {
  def converterToCassandra = new OptionToNullConverter(TypeConverter.forType[UDTValue])
  override def isCollection = false
  override def scalaTypeTag = TypeTag.synchronized { implicitly[TypeTag[UDTValue]] }
}
