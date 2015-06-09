package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import com.datastax.driver.core.{UserType, ProtocolVersion, DataType}
import com.datastax.spark.connector.util.Symbols

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/** Serializable representation of column data type. */
trait ColumnType[T] extends Serializable {

  /** Returns a converter that converts values to the Scala type associated with this column. */
  lazy val converterToScala: TypeConverter[T] =
    TypeConverter.forType(scalaTypeTag)

  /** Returns a converter that converts this column to type that can be saved by TableWriter. */
  def converterToCassandra: TypeConverter[_ <: AnyRef]

  /** Returns the TypeTag of the Scala type recommended to represent values of this column. */
  def scalaTypeTag: TypeTag[T]

  /** Name of the Scala type. Useful for source generation.*/
  def scalaTypeName: String
    = TypeTag.synchronized(scalaTypeTag.tpe.toString)

  /** Name of the CQL type. Useful for CQL generation.*/
  def cqlTypeName: String

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
  private def unlazify[T](seq: IndexedSeq[T]): IndexedSeq[T] = IndexedSeq(seq: _*)

  private def fields(dataType: UserType): IndexedSeq[UDTFieldDef] = unlazify {
    for (field <- dataType.iterator().toIndexedSeq) yield
      UDTFieldDef(field.getName, fromDriverType(field.getType))
  }

  def fromDriverType(dataType: DataType): ColumnType[_] = {
    val typeArgs = dataType.getTypeArguments.map(fromDriverType)
    (dataType, dataType.getName) match {
      case (_, DataType.Name.LIST) => ListType(typeArgs(0))
      case (_, DataType.Name.SET)  => SetType(typeArgs(0))
      case (_, DataType.Name.MAP)  => MapType(typeArgs(0), typeArgs(1))
      case (userType: UserType, _) => UserDefinedType(userType.getTypeName, fields(userType))
      case _ => primitiveTypeMap(dataType)
    }
  }

  /** Returns natural Cassandra type for representing data of the given Scala type */
  def fromScalaType(dataType: Type): ColumnType[_] = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    // can't use a HashMap, because there are more than one different Type objects for "real type":
    if (dataType =:= typeOf[Int]) IntType
    else if (dataType =:= typeOf[java.lang.Integer]) IntType
    else if (dataType =:= typeOf[Long]) BigIntType
    else if (dataType =:= typeOf[java.lang.Long]) BigIntType
    else if (dataType =:= typeOf[Float]) FloatType
    else if (dataType =:= typeOf[java.lang.Float]) FloatType
    else if (dataType =:= typeOf[Double]) DoubleType
    else if (dataType =:= typeOf[java.lang.Double]) DoubleType
    else if (dataType =:= typeOf[BigInt]) VarIntType
    else if (dataType =:= typeOf[java.math.BigInteger]) VarIntType
    else if (dataType =:= typeOf[BigDecimal]) DecimalType
    else if (dataType =:= typeOf[java.math.BigDecimal]) DecimalType
    else if (dataType =:= typeOf[Boolean]) BooleanType
    else if (dataType =:= typeOf[java.lang.Boolean]) BooleanType
    else if (dataType =:= typeOf[String]) VarCharType
    else if (dataType =:= typeOf[InetAddress]) InetType
    else if (dataType =:= typeOf[Date]) TimestampType
    else if (dataType =:= typeOf[java.sql.Date]) TimestampType
    else if (dataType =:= typeOf[org.joda.time.DateTime]) TimestampType
    else if (dataType =:= typeOf[UUID]) UUIDType
    else if (dataType =:= typeOf[ByteBuffer]) BlobType
    else if (dataType =:= typeOf[Array[Byte]]) BlobType
    else {
      dataType match {
        case TypeRef(_, symbol, List(arg)) =>
          val argType = fromScalaType(arg)
          if (symbol == Symbols.OptionSymbol)
            argType
          else if (Symbols.ListSymbols contains symbol)
            ListType(argType)
          else if (Symbols.SetSymbols contains symbol)
            SetType(argType)
          else
            unsupportedType()
        case TypeRef(_, symbol, List(k, v)) =>
          val keyType = fromScalaType(k)
          val valueType = fromScalaType(v)
          if (Symbols.MapSymbols contains symbol)
            MapType(keyType, valueType)
          else
            unsupportedType()
        case _ =>
          unsupportedType()
      }
    }
  }

  /** Returns a converter that converts values to the type of this column expected by the
    * Cassandra Java driver when saving the row.*/
  def converterToCassandra(dataType: DataType)(implicit protocolVersion: ProtocolVersion)
      : TypeConverter[_ <: AnyRef] = {

    val typeArgs = dataType.getTypeArguments.map(converterToCassandra)
    val converter: TypeConverter[_] =
      dataType.getName match {
        case DataType.Name.LIST => TypeConverter.javaArrayListConverter(typeArgs(0))
        case DataType.Name.SET => TypeConverter.javaHashSetConverter(typeArgs(0))
        case DataType.Name.MAP => TypeConverter.javaHashMapConverter(typeArgs(0), typeArgs(1))
        case DataType.Name.UDT => UserDefinedType.driverUDTValueConverter(dataType)
        case _ => fromDriverType(dataType).converterToCassandra
      }

    // make sure it is always wrapped in OptionToNullConverter, but don't wrap twice:
    converter match {
      case c: TypeConverter.OptionToNullConverter => c
      case _ => new TypeConverter.OptionToNullConverter(converter)
    }
  }

}

