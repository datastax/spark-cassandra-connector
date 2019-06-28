package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.types.{BooleanType, _}
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, _}
import org.junit.Assert._
import org.junit.Test

class DataFrameColumnMapperTest {

  @Test
  def testNewTable(): Unit = {
    val structType = StructType(Array(StructField("column1", IntegerType),
      StructField("column2", ArrayType(StringType)),
      StructField("column3", SparkSqlBooleanType)))

    val columnMapper = new DataFrameColumnMapper[DataFrameColumnMapperTest](structType)
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
