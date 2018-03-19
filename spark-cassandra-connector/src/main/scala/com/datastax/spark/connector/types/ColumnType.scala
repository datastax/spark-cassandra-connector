package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import org.apache.spark.SparkEnv
import com.datastax.driver.core.{DataType, ProtocolVersion, TupleType => DriverTupleType, UserType => DriverUserType}
import com.datastax.driver.core.ProtocolVersion._
import com.datastax.spark.connector.util.{ConfigParameter, ReflectionUtil, Symbols}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, DataType => SparkSqlDataType, DateType => SparkSqlDateType, DecimalType => SparkSqlDecimalType, DoubleType => SparkSqlDoubleType, FloatType => SparkSqlFloatType, MapType => SparkSqlMapType, TimestampType => SparkSqlTimestampType, _}

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
  def scalaTypeName: String = scalaTypeTag.tpe.toString

  /** Name of the CQL type. Useful for CQL generation.*/
  def cqlTypeName: String

  def isCollection: Boolean
}

object ColumnTypeConf {

  val ReferenceSection = "Custom Cassandra Type Parameters (Expert Use Only)"

  val CustomDriverTypeParam = ConfigParameter[Option[String]](
    name = "spark.cassandra.dev.customFromDriver",
    section = ReferenceSection,
    default = None,
    description = """Provides an additional class implementing CustomDriverConverter for those
        |clients that need to read non-standard primitive Cassandra types. If your Cassandra implementation
        |uses a Java Driver which can read DataType.custom() you may need it this. If you are using
        |OSS Cassandra this should never be used.""".stripMargin('|')
  )

  val Properties = Set(CustomDriverTypeParam)
}

object ColumnType {

  val protocolVersionOrdering = implicitly[Ordering[ProtocolVersion]]
  import protocolVersionOrdering._

  private[connector] val primitiveTypeMap = Map[DataType, ColumnType[_]](
    DataType.text() -> TextType,
    DataType.ascii() -> AsciiType,
    DataType.varchar() -> VarCharType,
    DataType.cint() -> IntType,
    DataType.bigint() -> BigIntType,
    DataType.smallint() -> SmallIntType,
    DataType.tinyint() -> TinyIntType,
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
    DataType.counter() -> CounterType,
    DataType.date() -> DateType,
    DataType.time() -> TimeType
  )

  lazy val customDriverConverter: Option[CustomDriverConverter] = {
    Option(SparkEnv.get)
      .flatMap(env => env.conf.getOption(ColumnTypeConf.CustomDriverTypeParam.name))
      .flatMap(className => Some(ReflectionUtil.findGlobalObject[CustomDriverConverter](className)))
  }

  private lazy val customFromDriverRow: PartialFunction[DataType, ColumnType[_]] = {
    customDriverConverter
      .flatMap(clazz => Some(clazz.fromDriverRowExtension))
      .getOrElse(PartialFunction.empty)
  }

  /** Makes sure the sequence does not contain any lazy transformations.
    * This guarantees that if T is Serializable, the collection is Serializable. */
  private def unlazify[T](seq: IndexedSeq[T]): IndexedSeq[T] = IndexedSeq(seq: _*)

  private def fields(dataType: DriverUserType): IndexedSeq[UDTFieldDef] = unlazify {
    for (field <- dataType.iterator().toIndexedSeq) yield
      UDTFieldDef(field.getName, fromDriverType(field.getType))
  }

  private def fields(dataType: DriverTupleType): IndexedSeq[TupleFieldDef] = unlazify {
    for ((field, index) <- dataType.getComponentTypes.toIndexedSeq.zipWithIndex) yield
      TupleFieldDef(index, fromDriverType(field))
  }

  private def typeArg(dataType: DataType, idx: Int) = fromDriverType(dataType.getTypeArguments.get(idx))

  private val standardFromDriverRow: PartialFunction[DataType, ColumnType[_]] = {
    case listType if listType.getName == DataType.Name.LIST => ListType(typeArg(listType, 0))
    case setType if setType.getName == DataType.Name.SET => SetType(typeArg(setType, 0))
    case mapType if mapType.getName == DataType.Name.MAP => MapType(typeArg(mapType, 0), typeArg(mapType, 1))
    case userType: DriverUserType => UserDefinedType(userType.getTypeName, fields(userType))
    case tupleType: DriverTupleType => TupleType(fields(tupleType): _*)
    case dataType => primitiveTypeMap(dataType)
  }

  def fromDriverType(dataType: DataType): ColumnType[_] = {
    val getColumnType: PartialFunction[DataType, ColumnType[_]] = customFromDriverRow orElse standardFromDriverRow
    getColumnType(dataType)
  }

  /** Returns natural Cassandra type for representing data of the given Spark SQL type */
  def fromSparkSqlType(
    dataType: SparkSqlDataType,
    protocolVersion: ProtocolVersion = ProtocolVersion.NEWEST_SUPPORTED): ColumnType[_] = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    dataType match {
      case ByteType => if (protocolVersion >= V4) TinyIntType else IntType
      case ShortType => if (protocolVersion >= V4) SmallIntType else IntType
      case IntegerType => IntType
      case LongType => BigIntType
      case SparkSqlFloatType => FloatType
      case SparkSqlDoubleType => DoubleType
      case StringType => VarCharType
      case BinaryType => BlobType
      case SparkSqlBooleanType => BooleanType
      case SparkSqlTimestampType => TimestampType
      case SparkSqlDateType => if (protocolVersion >= V4) DateType else TimestampType
      case SparkSqlDecimalType() => DecimalType
      case ArrayType(sparkSqlElementType, containsNull) =>
        val argType = fromSparkSqlType(sparkSqlElementType)
        ListType(argType)
      case SparkSqlMapType(sparkSqlKeyType, sparkSqlValueType, containsNull) =>
        val keyType = fromSparkSqlType(sparkSqlKeyType)
        val valueType = fromSparkSqlType(sparkSqlValueType)
        MapType(keyType, valueType)
      case _ =>
        unsupportedType()
    }
  }

  /** Returns natural Cassandra type for representing data of the given Scala type */
  def fromScalaType(
    dataType: Type,
    protocolVersion: ProtocolVersion = ProtocolVersion.NEWEST_SUPPORTED): ColumnType[_] = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    // can't use a HashMap, because there are more than one different Type objects for "real type":
    if (dataType =:= typeOf[Int]) IntType
    else if (dataType =:= typeOf[java.lang.Integer]) IntType
    else if (dataType =:= typeOf[Long]) BigIntType
    else if (dataType =:= typeOf[java.lang.Long]) BigIntType
    else if (dataType =:= typeOf[Short]) if (protocolVersion >= V4) SmallIntType else IntType
    else if (dataType =:= typeOf[java.lang.Short]) if (protocolVersion >= V4) SmallIntType else IntType
    else if (dataType =:= typeOf[Byte]) if (protocolVersion >=  V4) TinyIntType else IntType
    else if (dataType =:= typeOf[java.lang.Byte]) if (protocolVersion >= V4) TinyIntType else IntType
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
    else if (dataType =:= typeOf[java.sql.Date]) if (protocolVersion >= V4) DateType else TimestampType
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
  def converterToCassandra(dataType: DataType)
      : TypeConverter[_ <: AnyRef] = {

    val typeArgs = dataType.getTypeArguments.map(converterToCassandra)
    val converter: TypeConverter[_] =
      dataType.getName match {
        case DataType.Name.LIST => TypeConverter.javaArrayListConverter(typeArgs(0))
        case DataType.Name.SET => TypeConverter.javaHashSetConverter(typeArgs(0))
        case DataType.Name.MAP => TypeConverter.javaHashMapConverter(typeArgs(0), typeArgs(1))
        case DataType.Name.UDT => UserDefinedType.driverUDTValueConverter(dataType)
        case DataType.Name.TUPLE => TupleType.driverTupleValueConverter(dataType)
        case _ => fromDriverType(dataType).converterToCassandra
      }

    // make sure it is always wrapped in OptionToNullConverter, but don't wrap twice:
    converter match {
      case c: TypeConverter.OptionToNullConverter => c
      case _ => new TypeConverter.OptionToNullConverter(converter)
    }
  }

}

