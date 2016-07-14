package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.datastax.driver.core.ProtocolVersion
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, DateType => SparkSqlDateType, DecimalType => SparkSqlDecimalType, DoubleType => SparkSqlDoubleType, FloatType => SparkSqlFloatType, MapType => SparkSqlMapType, TimestampType => SparkSqlTimestampType, _}

import scala.reflect.runtime.universe._
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}

class ColumnTypeSpec extends WordSpec with Matchers with GivenWhenThen {

  "A ColumnType companion object" should {

    "throw InvalidArgumentException if given unsupported type" in {
      an [IllegalArgumentException] should be thrownBy ColumnType.fromScalaType(typeOf[ColumnTypeSpec])
    }

    "allow to obtain a proper ColumnType" when {

      "given a Boolean should return BooleanType" in {
        assert (ColumnType.fromScalaType(typeOf[Boolean]) === BooleanType)
      }
      "given a java.lang.Boolean should return BooleanType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Boolean]) === BooleanType)
      }
      "given an Int should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Int]) === IntType)
      }
      "given an java.lang.Integer should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Integer]) === IntType)
      }
      "given a Long should return BigIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Long]) === BigIntType)
      }
      "given a java.lang.Long should return BigIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Long]) === BigIntType)
      }
      "given a Float should return FloatType" in {
        assert (ColumnType.fromScalaType(typeOf[Float]) === FloatType)
      }
      "given a java.lang.Float should return FloatType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Float]) === FloatType)
      }
      "given a Double should return DoubleType" in {
        assert (ColumnType.fromScalaType(typeOf[Double]) === DoubleType)
      }
      "given a java.lang.Double should return DoubleType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Double]) === DoubleType)
      }
      "given a String should return VarcharType" in {
        assert (ColumnType.fromScalaType(typeOf[String]) === VarCharType)
      }
      "given a java.lang.Short should return a SmallIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Short]) === SmallIntType)
      }
      "given a Short should return a SmallIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Short]) === SmallIntType)
      }
      "given a Byte should return a TinyIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Byte]) === TinyIntType)
      }
      "given a java.lang.Byte should return a TinyIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Byte]) === TinyIntType)
      }
      "given a java.lang.Short and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Short], ProtocolVersion.V3) === IntType)
      }
      "given a Short and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Short], ProtocolVersion.V3) === IntType)
      }
      "given a Byte and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Byte], ProtocolVersion.V3) === IntType)
      }
      "given a java.lang.Byte and PV3 should return a IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Byte], ProtocolVersion.V3) === IntType)
      }
      "given a java.util.Date should return TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[java.util.Date]) === TimestampType)
      }
      "given a java.sql.Date should return DateType" in {
        assert (ColumnType.fromScalaType(typeOf[java.sql.Date]) === DateType)
      }
      "given a java.sql.Date and a pre V4 Protcol version should return a TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[java.sql.Date], ProtocolVersion.V3) === TimestampType)
      }
      "given a org.joda.time.DateTime should return TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[org.joda.time.DateTime]) === TimestampType)
      }
      "given a ByteBuffer should return BlobType" in {
        assert (ColumnType.fromScalaType(typeOf[ByteBuffer]) === BlobType)
      }
      "given an Array[Byte] should return BlobType" in {
        assert (ColumnType.fromScalaType(typeOf[Array[Byte]]) === BlobType)
      }
      "given an UUID should return UUIDType" in {
        assert (ColumnType.fromScalaType(typeOf[UUID]) === UUIDType)
      }
      "given a List[String] should return ListType(VarcharType)" in {
        assert (ColumnType.fromScalaType(typeOf[List[String]]) === ListType(VarCharType))
      }
      "given a Set[InetAddress] should return SetType(InetType)" in {
        assert (ColumnType.fromScalaType(typeOf[Set[InetAddress]]) === SetType(InetType))
      }
      "given a Map[Int, Date] should return MapType(IntType, TimestampType)" in {
        assert (ColumnType.fromScalaType(typeOf[Map[Int, Date]]) === MapType(IntType, TimestampType))
      }
      "given an Option[Int] should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Option[Int]]) === IntType)
      }
      "given an Option[Vector[Int]] should return ListType(IntType)" in {
        assert (ColumnType.fromScalaType(typeOf[Option[Vector[Int]]]) === ListType(IntType))
      }
    }

    "allow to obtain a proper ColumnType from Spark SQL type" when {

      "given a ByteType should return TinyIntType" in {
        assert (ColumnType.fromSparkSqlType(ByteType) === TinyIntType)
      }
      "given a ShortType should return SmallIntType" in {
        assert (ColumnType.fromSparkSqlType(ShortType) === SmallIntType)
      }
      "given a ByteType and PV3 should return IntType" in {
        assert (ColumnType.fromSparkSqlType(ByteType, ProtocolVersion.V3) === IntType)
      }
      "given a ShortType and PV3 should return IntType" in {
        assert (ColumnType.fromSparkSqlType(ShortType, ProtocolVersion.V3) === IntType)
      }
      "given a BooleanType should return BooleanType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlBooleanType) === BooleanType)
      }
      "given an IntegerType should return IntType" in {
        assert (ColumnType.fromSparkSqlType(IntegerType) === IntType)
      }
      "given a LongType should return BigIntType" in {
        assert (ColumnType.fromSparkSqlType(LongType) === BigIntType)
      }
      "given a FloatType should return FloatType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlFloatType) === FloatType)
      }
      "given a DoubleType should return DoubleType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlDoubleType) === DoubleType)
      }
      "given a DecimalType should return DecimalType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlDecimalType(10,0)) === DecimalType)
      }
      "given a StringType should return VarcharType" in {
        assert (ColumnType.fromSparkSqlType(StringType) === VarCharType)
      }
      "given a TimestampType should return TimestampType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlTimestampType) === TimestampType)
      }
      "given a SparkSqlDateType should return DateType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlDateType) === DateType)
      }
      "given a SparkSqlDateType and PV3 should return TimestampType" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlDateType, ProtocolVersion.V3) === TimestampType)
      }
      "given a BinaryType should return BlobType" in {
        assert (ColumnType.fromSparkSqlType(BinaryType) === BlobType)
      }
      "given a ArrayType(String) should return ListType(VarcharType)" in {
        assert (ColumnType.fromSparkSqlType(ArrayType(StringType)) === ListType(VarCharType))
      }
      "given a MapType(IntegerType, SparkSqlDateType) should return MapType(IntType, DateTypeType)" in {
        assert (ColumnType.fromSparkSqlType(SparkSqlMapType(IntegerType, SparkSqlDateType))
            === MapType(IntType, DateType))
      }
    }
  }

}
