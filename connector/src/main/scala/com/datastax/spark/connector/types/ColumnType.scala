package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.datastax.dse.driver.api.core.`type`.DseDataTypes
import com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4
import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.{
  DataType,
  DataTypes => DriverDataTypes,
  TupleType => DriverTupleType,
  UserDefinedType => DriverUserDefinedType,
  ListType => DriverListType,
  MapType => DriverMapType,
  SetType => DriverSetType}
import com.datastax.spark.connector.util._
import org.apache.spark.sql.types.{
  BooleanType => SparkSqlBooleanType,
  DataType => SparkSqlDataType,
  DateType => SparkSqlDateType,
  DecimalType => SparkSqlDecimalType,
  DoubleType => SparkSqlDoubleType,
  FloatType => SparkSqlFloatType,
  MapType => SparkSqlMapType,
  TimestampType => SparkSqlTimestampType, _}

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
    = scalaTypeTag.tpe.toString

  /** Name of the CQL type. Useful for CQL generation.*/
  def cqlTypeName: String

  def isCollection: Boolean

  def isFrozen: Boolean = false

  def isMultiCell: Boolean = isCollection && !isFrozen
}

object ColumnTypeConf {

  val ReferenceSection = "Custom Cassandra Type Parameters (Expert Use Only)"

  val deprecatedCustomDriverTypeParam = DeprecatedConfigParameter(
    "spark.cassandra.dev.customFromDriver",
    None,
    deprecatedSince = "Analytics Connector 1.0",
    rational = "The ability to load new driver type converters at runtime has been removed"
  )

}

case class ColumnTypeConf(customFromDriver: Option[String])

object ColumnType {

  private[connector] val primitiveTypeMap = Map[DataType, ColumnType[_]](
    DriverDataTypes.TEXT -> TextType,
    DriverDataTypes.ASCII -> AsciiType,
    DriverDataTypes.TEXT -> VarCharType,
    DriverDataTypes.INT -> IntType,
    DriverDataTypes.BIGINT -> BigIntType,
    DriverDataTypes.SMALLINT -> SmallIntType,
    DriverDataTypes.TINYINT -> TinyIntType,
    DriverDataTypes.FLOAT -> FloatType,
    DriverDataTypes.DOUBLE -> DoubleType,
    DriverDataTypes.BOOLEAN -> BooleanType,
    DriverDataTypes.VARINT -> VarIntType,
    DriverDataTypes.DECIMAL -> DecimalType,
    DriverDataTypes.TIMESTAMP -> TimestampType,
    DriverDataTypes.INET -> InetType,
    DriverDataTypes.UUID -> UUIDType,
    DriverDataTypes.TIMEUUID -> TimeUUIDType,
    DriverDataTypes.BLOB -> BlobType,
    DriverDataTypes.COUNTER -> CounterType,
    DriverDataTypes.DATE -> DateType,
    DriverDataTypes.TIME -> TimeType,
    DriverDataTypes.DURATION -> DurationType,
    DseDataTypes.POINT -> PointType,
    DseDataTypes.POLYGON -> PolygonType,
    DseDataTypes.LINE_STRING -> LineStringType
  )

  /** Makes sure the sequence does not contain any lazy transformations.
    * This guarantees that if T is Serializable, the collection is Serializable. */
  private def unlazify[T](seq: IndexedSeq[T]): IndexedSeq[T] = IndexedSeq(seq: _*)

  private def fields(dataType: DriverUserDefinedType): IndexedSeq[UDTFieldDef] = unlazify {
    for ((fieldName, fieldType) <- dataType.getFieldNames.zip(dataType.getFieldTypes).toIndexedSeq) yield
      UDTFieldDef(fieldName.asInternal(), fromDriverType(fieldType))
  }

  private def fields(dataType: DriverTupleType): IndexedSeq[TupleFieldDef] = unlazify {
    for ((field, index) <- dataType.getComponentTypes.toIndexedSeq.zipWithIndex) yield
      TupleFieldDef(index, fromDriverType(field))
  }

  private val standardFromDriverRow: PartialFunction[DataType, ColumnType[_]] = {
    case listType: DriverListType => ListType(fromDriverType(listType.getElementType), listType.isFrozen)
    case setType: DriverSetType => SetType(fromDriverType(setType.getElementType), setType.isFrozen)
    case mapType: DriverMapType => MapType(fromDriverType(mapType.getKeyType), fromDriverType(mapType.getValueType), mapType.isFrozen)
    case userType: DriverUserDefinedType => UserDefinedType(userType.getName.asInternal(), fields(userType), userType.isFrozen)
    case tupleType: DriverTupleType => TupleType(fields(tupleType): _*)
    case dataType => primitiveTypeMap(dataType)
  }

  def fromDriverType(dataType: DataType): ColumnType[_] = {
    standardFromDriverRow(dataType)
  }

  /** Returns natural Cassandra type for representing data of the given Spark SQL type */
  def fromSparkSqlType(
    dataType: SparkSqlDataType,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): ColumnType[_] = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    val pvGt4 = (protocolVersion.getCode >= V4.getCode)

    dataType match {
      case ByteType => if (pvGt4) TinyIntType else IntType
      case ShortType => if (pvGt4) SmallIntType else IntType
      case IntegerType => IntType
      case LongType => BigIntType
      case SparkSqlFloatType => FloatType
      case SparkSqlDoubleType => DoubleType
      case StringType => VarCharType
      case BinaryType => BlobType
      case SparkSqlBooleanType => BooleanType
      case SparkSqlTimestampType => TimestampType
      case SparkSqlDateType => if (pvGt4) DateType else TimestampType
      case SparkSqlDecimalType() => DecimalType
      case ArrayType(sparkSqlElementType, containsNull) =>
        val argType = fromSparkSqlType(sparkSqlElementType)
        ListType(argType, false)
      case SparkSqlMapType(sparkSqlKeyType, sparkSqlValueType, containsNull) =>
        val keyType = fromSparkSqlType(sparkSqlKeyType)
        val valueType = fromSparkSqlType(sparkSqlValueType)
        MapType(keyType, valueType, false)
      case _ =>
        unsupportedType()
    }
  }

  /** Returns natural Cassandra type for representing data of the given Scala type */
  def fromScalaType(
    dataType: Type,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): ColumnType[_] = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    val pvGt4 = (protocolVersion.getCode >= V4.getCode)

    // can't use a HashMap, because there are more than one different Type objects for "real type":
    if (dataType =:= typeOf[Int]) IntType
    else if (dataType =:= typeOf[java.lang.Integer]) IntType
    else if (dataType =:= typeOf[Long]) BigIntType
    else if (dataType =:= typeOf[java.lang.Long]) BigIntType
    else if (dataType =:= typeOf[Short]) if (pvGt4) SmallIntType else IntType
    else if (dataType =:= typeOf[java.lang.Short]) if (pvGt4) SmallIntType else IntType
    else if (dataType =:= typeOf[Byte]) if (pvGt4) TinyIntType else IntType
    else if (dataType =:= typeOf[java.lang.Byte]) if (pvGt4) TinyIntType else IntType
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
    else if (dataType =:= typeOf[java.sql.Timestamp]) TimestampType
    else if (dataType =:= typeOf[Date]) TimestampType
    else if (dataType =:= typeOf[java.sql.Date]) if (pvGt4) DateType else TimestampType
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
            ListType(argType, false)
          else if (Symbols.SetSymbols contains symbol)
            SetType(argType, false)
          else
            unsupportedType()
        case TypeRef(_, symbol, List(k, v)) =>
          val keyType = fromScalaType(k)
          val valueType = fromScalaType(v)
          if (Symbols.MapSymbols contains symbol)
            MapType(keyType, valueType, false)
          else
            unsupportedType()
        case _ =>
          unsupportedType()
      }
    }
  }

  /** Returns a converter that converts values to the type of this column expected by the
    * Cassandra Java driver when saving the row.*/
  def converterToCassandra(dataType: DataType)
      : TypeConverter[_ <: AnyRef] = {

    val converter: TypeConverter[_] =
      dataType match {
        case list: DriverListType => TypeConverter.javaArrayListConverter(converterToCassandra(list.getElementType))
        case set: DriverSetType => TypeConverter.javaHashSetConverter(converterToCassandra(set.getElementType))
        case map: DriverMapType => TypeConverter.javaHashMapConverter(converterToCassandra(map.getKeyType), converterToCassandra(map.getValueType))
        case udt: DriverUserDefinedType => new UserDefinedType.DriverUDTValueConverter(udt)
        case tuple: DriverTupleType => TupleType.driverTupleValueConverter(dataType)
        case _ => fromDriverType(dataType).converterToCassandra
      }

    // make sure it is always wrapped in OptionToNullConverter, but don't wrap twice:
    converter match {
      case c: TypeConverter.OptionToNullConverter => c
      case _ => new TypeConverter.OptionToNullConverter(converter)
    }
  }

}

