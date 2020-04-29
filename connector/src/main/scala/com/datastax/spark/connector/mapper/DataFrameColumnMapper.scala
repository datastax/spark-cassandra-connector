package com.datastax.spark.connector.mapper

import com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4
import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{BigIntType, BlobType, BooleanType, ColumnType, DateType, DecimalType, DoubleType, FloatType, IntType, ListType, MapType, SmallIntType, TimestampType, TinyIntType, VarCharType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{
  BooleanType => SparkSqlBooleanType,
  DataType => SparkSqlDataType,
  DateType => SparkSqlDateType,
  DecimalType => SparkSqlDecimalType,
  DoubleType => SparkSqlDoubleType,
  FloatType => SparkSqlFloatType,
  MapType => SparkSqlMapType,
  TimestampType => SparkSqlTimestampType, _}

class DataFrameColumnMapper[T](structType: StructType) extends ColumnMapper[T] {
  override def columnMapForWriting(struct: StructDef,
                                   selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = ???

  override def columnMapForReading(struct: StructDef,
                                   selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = ???

  override def newTable(
    keyspaceName: String,
    tableName: String,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): TableDescriptor = {

    val columns = structType.zipWithIndex.map { case (field:StructField, i:Int) =>
      ColumnDescriptor(field.name,
        DataFrameColumnMapper.fromSparkSqlType(field.dataType, protocolVersion),
        i == 0,
        false)
      }

    TableDescriptor(keyspaceName, tableName, columns)
  }
}

object DataFrameColumnMapper {
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
        ListType(argType, isFrozen = false)
      case SparkSqlMapType(sparkSqlKeyType, sparkSqlValueType, containsNull) =>
        val keyType = fromSparkSqlType(sparkSqlKeyType)
        val valueType = fromSparkSqlType(sparkSqlValueType)
        MapType(keyType, valueType, isFrozen = false)
      case _ =>
        unsupportedType()
    }
  }
}