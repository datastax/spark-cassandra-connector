package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.cql.{TableDef, RegularColumn, ColumnDef}
import com.datastax.spark.connector.types.IntType
import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test

class TupleColumnMapperTest {

  private val c1 = ColumnDef("test", "table", "column1", RegularColumn, IntType)
  private val c2 = ColumnDef("test", "table", "column2", RegularColumn, IntType)
  private val c3 = ColumnDef("test", "table", "column3", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3))

  @Test
  def testGetters() {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)].columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(IndexedColumnRef(0), getters("_1"))
    assertEquals(IndexedColumnRef(1), getters("_2"))
    assertEquals(IndexedColumnRef(2), getters("_3"))
  }

  @Test
  def testConstructor() {
    val columnMap = new TupleColumnMapper[(Int, String, Boolean)].columnMap(tableDef)
    assertEquals(Seq(IndexedColumnRef(0), IndexedColumnRef(1), IndexedColumnRef(2)), columnMap.constructor)
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
    assertEquals(IndexedColumnRef(0), getters("_1"))
    assertEquals(IndexedColumnRef(1), getters("_2"))
    assertEquals(IndexedColumnRef(2), getters("_3"))
  }

}
