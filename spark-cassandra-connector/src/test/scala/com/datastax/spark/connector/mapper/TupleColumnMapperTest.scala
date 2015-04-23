package com.datastax.spark.connector.mapper

import java.util.Date

import com.datastax.spark.connector.ColumnIndex
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._
import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test

class TupleColumnMapperTest {

  private val c1 = ColumnDef("column1", PartitionKeyColumn, IntType)
  private val c2 = ColumnDef("column2", ClusteringColumn(0), IntType)
  private val c3 = ColumnDef("column3", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3))

  @Test
  def testGetters() {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)].columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(ColumnIndex(0), getters("_1"))
    assertEquals(ColumnIndex(1), getters("_2"))
    assertEquals(ColumnIndex(2), getters("_3"))
  }

  @Test
  def testConstructor() {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)].columnMap(tableDef)
    assertEquals(Seq(ColumnIndex(0), ColumnIndex(1), ColumnIndex(2)), columnMap.constructor)
  }

  @Test
  def testSerialize() {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)].columnMap(tableDef)
    SerializationUtils.roundtrip(columnMap)
  }

  @Test
  def testImplicit() {
    val columnMap = implicitly[ColumnMapper[(Int, String, Boolean)]].columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(ColumnIndex(0), getters("_1"))
    assertEquals(ColumnIndex(1), getters("_2"))
    assertEquals(ColumnIndex(2), getters("_3"))
  }

  @Test
  def testNewTable(): Unit = {
    val columnMapper = new TupleColumnMapper[(Int, String, Boolean, List[Date])]
    val table = columnMapper.newTable("keyspace", "table")
    assertEquals("keyspace", table.keyspaceName)
    assertEquals("table", table.tableName)
    assertEquals(4, table.columns.size)
    assertEquals(1, table.partitionKey.size)
    assertEquals(IntType, table.partitionKey(0).columnType)
    assertEquals(VarCharType, table.columns(1).columnType)
    assertEquals(BooleanType, table.columns(2).columnType)
    assertEquals(ListType(TimestampType), table.columns(3).columnType)
  }

}
