package com.datastax.spark.connector.mapper

import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.spark.connector.types.{BooleanType, DateType, _}
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, DateType => SparkSqlDateType, DecimalType => SparkSqlDecimalType, DoubleType => SparkSqlDoubleType, FloatType => SparkSqlFloatType, MapType => SparkSqlMapType, TimestampType => SparkSqlTimestampType, _}
import org.junit.Assert._
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}

class DataFrameColumnMapperSpec extends WordSpec with Matchers with GivenWhenThen {

  "DataFrameColumnMapper" should {

    "allow to obtain a proper ColumnType from Spark SQL type" when {

      "given a ByteType should return TinyIntType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(ByteType) === TinyIntType)
      }
      "given a ShortType should return SmallIntType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(ShortType) === SmallIntType)
      }
      "given a ByteType and PV3 should return IntType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(ByteType, DefaultProtocolVersion.V3) === IntType)
      }
      "given a ShortType and PV3 should return IntType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(ShortType, DefaultProtocolVersion.V3) === IntType)
      }
      "given a BooleanType should return BooleanType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlBooleanType) === BooleanType)
      }
      "given an IntegerType should return IntType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(IntegerType) === IntType)
      }
      "given a LongType should return BigIntType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(LongType) === BigIntType)
      }
      "given a FloatType should return FloatType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlFloatType) === FloatType)
      }
      "given a DoubleType should return DoubleType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlDoubleType) === DoubleType)
      }
      "given a DecimalType should return DecimalType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlDecimalType(10, 0)) === DecimalType)
      }
      "given a StringType should return VarcharType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(StringType) === VarCharType)
      }
      "given a TimestampType should return TimestampType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlTimestampType) === TimestampType)
      }
      "given a SparkSqlDateType should return DateType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlDateType) === DateType)
      }
      "given a SparkSqlDateType and PV3 should return TimestampType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlDateType, DefaultProtocolVersion.V3) === TimestampType)
      }
      "given a BinaryType should return BlobType" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(BinaryType) === BlobType)
      }
      "given a ArrayType(String) should return ListType(VarcharType)" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(ArrayType(StringType)) === ListType(VarCharType))
      }
      "given a MapType(IntegerType, SparkSqlDateType) should return MapType(IntType, DateTypeType)" in {
        assert(DataFrameColumnMapper.fromSparkSqlType(SparkSqlMapType(IntegerType, SparkSqlDateType))
          === MapType(IntType, DateType))
      }
      "create new table" in {
        val structType = StructType(Array(StructField("column1", IntegerType),
          StructField("column2", ArrayType(StringType)),
          StructField("column3", SparkSqlBooleanType)))

        val columnMapper = new DataFrameColumnMapper[DataFrameColumnMapperSpec](structType)
        val table = columnMapper.newTable("keyspace", "table")
        assertEquals("keyspace", table.keyspaceName)
        assertEquals("table", table.tableName)
        assertEquals(3, table.columns.size)
        assertEquals(1, table.partitionKey.size)
        assertEquals(IntType, table.partitionKey(0).columnType)
        assertEquals(ListType(VarCharType), table.columns(1).columnType)
        assertEquals(BooleanType, table.columns(2).columnType)
      }
    }
  }
}
