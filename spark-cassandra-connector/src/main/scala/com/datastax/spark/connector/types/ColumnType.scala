package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import com.datastax.driver.core.{UserType, ProtocolVersion, DataType}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/** Serializable representation of column data type. */
trait ColumnType[T] extends Serializable {

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
    DataType.timeuuid() -> TimeUUIDType,
    DataType.blob() -> BlobType,
    DataType.counter() -> CounterType
  )

  /** Makes sure the sequence does not contain any lazy transformations.
    * This guarantees that if T is Serializable, the collection is Serializable. */
  private def unlazify[T](seq: Seq[T]): Seq[T] = Seq(seq: _*)

  private def fields(dataType: UserType): Seq[FieldDef] = unlazify {
    for (field <- dataType.iterator().toSeq) yield
      FieldDef(field.getName, fromDriverType(field.getType))
  }

  def fromDriverType(dataType: DataType): ColumnType[_] = {
    val typeArgs = dataType.getTypeArguments.map(fromDriverType)
    (dataType, dataType.getName) match {
      case (_, DataType.Name.LIST) => ListType(typeArgs(0))
      case (_, DataType.Name.SET)  => SetType(typeArgs(0))
      case (_, DataType.Name.MAP)  => MapType(typeArgs(0), typeArgs(1))
      case (userType: UserType, _) => UserDefinedType(fields(userType))
      case _ => primitiveTypeMap(dataType)
    }
  }

  // Lambdas are used here instead of TypeConverter instances, because a user can chain custom
  // type converters for the builtin types by calling TypeConverter.registerConverter.
  // If we instantiated converters in advance, using user defined converters would not be possible.
  private val primitiveConverterMap = Map[DataType, () => TypeConverter[_]](
    DataType.text() -> { () => TypeConverter.forType[String] },
    DataType.ascii() -> { () => TypeConverter.forType[String] },
    DataType.varchar() -> { () => TypeConverter.forType[String] },
    DataType.cint() -> { () => TypeConverter.forType[java.lang.Integer] },
    DataType.bigint() -> { () => TypeConverter.forType[java.lang.Long] },
    DataType.cfloat() -> { () => TypeConverter.forType[java.lang.Float] },
    DataType.cdouble() -> { () => TypeConverter.forType[java.lang.Double] },
    DataType.cboolean() -> { () => TypeConverter.forType[java.lang.Boolean] },
    DataType.varint() -> { () => TypeConverter.forType[java.math.BigInteger] },
    DataType.decimal() -> { () => TypeConverter.forType[java.math.BigDecimal] },
    DataType.timestamp() -> { () => TypeConverter.forType[Date] },
    DataType.inet() -> { () => TypeConverter.forType[InetAddress] },
    DataType.uuid() -> { () => TypeConverter.forType[UUID] },
    DataType.timeuuid() -> { () => TypeConverter.forType[UUID] },
    DataType.blob() -> { () => TypeConverter.forType[ByteBuffer] },
    DataType.counter() -> { () => TypeConverter.forType[java.lang.Long] }
  )


  /** Returns a converter that converts values to the type of this column expected by the
    * Cassandra Java driver when saving the row.*/
  def converterToCassandra(dataType: DataType)(implicit protocolVersion: ProtocolVersion): TypeConverter[_ <: AnyRef] = {
    val typeArgs = dataType.getTypeArguments.map(converterToCassandra)
    val converter: TypeConverter[_] =
      dataType.getName match {
        case DataType.Name.LIST => TypeConverter.javaArrayListConverter(typeArgs(0))
        case DataType.Name.SET => TypeConverter.javaHashSetConverter(typeArgs(0))
        case DataType.Name.MAP => TypeConverter.javaHashMapConverter(typeArgs(0), typeArgs(1))
        case DataType.Name.UDT => UserDefinedType.driverUDTValueConverter(dataType)
        case _ => primitiveConverterMap(dataType)()
      }
    new TypeConverter.OptionToNullConverter(converter)
  }

}

