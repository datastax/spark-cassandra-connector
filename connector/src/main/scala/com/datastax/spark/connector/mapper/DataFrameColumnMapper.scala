package com.datastax.spark.connector.mapper

import com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4
import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.datasource.CassandraSourceUtil
import com.datastax.spark.connector.types.{BigIntType, BlobType, BooleanType, ColumnType, DateType, DecimalType, DoubleType, FloatType, IntType, ListType, MapType, SmallIntType, TimestampType, TinyIntType, VarCharType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, DataType => SparkSqlDataType, DateType => SparkSqlDateType, DecimalType => SparkSqlDecimalType, DoubleType => SparkSqlDoubleType, FloatType => SparkSqlFloatType, MapType => SparkSqlMapType, TimestampType => SparkSqlTimestampType, _}

class DataFrameColumnMapper[T](structType: StructType) extends ColumnMapper[T] {
  override def columnMapForWriting(struct: StructDef,
                                   selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = ???

  override def columnMapForReading(struct: StructDef,
                                   selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = ???

  override def newTable(
    keyspaceName: String,
    tableName: String,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): TableDef = {

    val columns = structType.zipWithIndex.map { case (field, i) => {
      val columnRole = if (i == 0) PartitionKeyColumn else RegularColumn
      ColumnDef(field.name, columnRole, ColumnType.fromDriverType(CassandraSourceUtil.sparkSqlToJavaDriverType(field.dataType, protocolVersion)))
    }}

    TableDef(keyspaceName, tableName, Seq(columns.head), Seq.empty, columns.tail)
  }
}
