package com.datastax.spark.connector.mapper

import com.datastax.spark.connector._
import com.datastax.spark.connector.{AllColumns, ColumnName}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.IntType
import org.apache.commons.lang3.SerializationUtils

import org.junit.Assert._
import org.junit.Test

class JavaBeanColumnMapperTestClass {
  def getProperty1: String = ???
  def setProperty1(str: String): Unit = ???

  def getCamelCaseProperty: Int = ???
  def setCamelCaseProperty(str: Int): Unit = ???

  def isFlagged: Boolean = ???
  def setFlagged(flag: Boolean): Unit = ???
}

class JavaBeanWithWeirdProps {
  def getDevil: Int = ???
  def setDevil(value: Int): Unit = ???

  def getCat: Int = ???
  def setCat(value: Int): Unit = ???

  def getEye: Int = ???
  def setEye(value: Int): Unit = ???
}


object JavaBeanColumnMapperTestClass {
  implicit object Mapper extends JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]()
}

class JavaBeanColumnMapperTest {

  private val c1 = ColumnDef("property_1", PartitionKeyColumn, IntType)
  private val c2 = ColumnDef("camel_case_property", ClusteringColumn(0), IntType)
  private val c3 = ColumnDef("flagged", RegularColumn, IntType)
  private val c4 = ColumnDef("marked", RegularColumn, IntType)
  private val c5 = ColumnDef("column", RegularColumn, IntType)
  private val table1 = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3))
  private val table2 = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3, c4, c5))

  @Test
  def testGetters() {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]
      .columnMapForWriting(table1, table1.columnRefs
      )
    val getters = columnMap.getters
    assertEquals(ColumnName(c1.columnName), getters("getProperty1"))
    assertEquals(ColumnName(c2.columnName), getters("getCamelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("isFlagged"))
  }

  @Test
  def testSetters() {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]
      .columnMapForReading(table1, table1.columnRefs)
    val setters = columnMap.setters
    assertEquals(ColumnName(c1.columnName), setters("setProperty1"))
    assertEquals(ColumnName(c2.columnName), setters("setCamelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), setters("setFlagged"))
  }

  @Test
  def testColumnNameOverrideGetters() {
    val columnNameOverrides: Map[String, String] = Map("property1" -> c5.columnName, "flagged" -> c4.columnName)
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass](columnNameOverrides)
      .columnMapForWriting(table2, IndexedSeq(c5.ref, c2.ref, c4.ref))
    val getters = columnMap.getters
    assertEquals(ColumnName(c5.columnName), getters("getProperty1"))
    assertEquals(ColumnName(c2.columnName), getters("getCamelCaseProperty"))
    assertEquals(ColumnName(c4.columnName), getters("isFlagged"))
  }

  @Test
  def testColumnNameOverrideSetters() {
    val columnNameOverrides: Map[String, String] = Map("property1" -> c5.columnName, "flagged" -> c4.columnName)
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass](columnNameOverrides)
      .columnMapForReading(table2, IndexedSeq(c5.ref, c2.ref, c4.ref))
    val setters = columnMap.setters
    assertEquals(ColumnName(c5.columnName), setters("setProperty1"))
    assertEquals(ColumnName(c2.columnName), setters("setCamelCaseProperty"))
    assertEquals(ColumnName(c4.columnName), setters("setFlagged"))
  }

  @Test
  def testSerializeColumnMap() {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]
      .columnMapForReading(table1, table1.columnRefs)
    SerializationUtils.roundtrip(columnMap)
  }

  @Test
  def testImplicit() {
    val mapper = implicitly[ColumnMapper[JavaBeanColumnMapperTestClass]]
    assertTrue(mapper.isInstanceOf[JavaBeanColumnMapper[_]])
  }

  @Test
  def testWorkWithAliases() {
    val mapper = new JavaBeanColumnMapper[ClassWithWeirdProps]()
    val selectedColumns = IndexedSeq(
      "property_1" as "devil",
      "camel_case_property" as "cat",
      "column" as "eye")
    val map = mapper.columnMapForReading(table2, selectedColumns)
    assertEquals(selectedColumns, map.constructor)
  }

  @Test
  def testWorkWithAliasesAndHonorOverrides() {
    val mapper = new JavaBeanColumnMapper[ClassWithWeirdProps](Map("cat" -> "other"))
    val selectedColumns = IndexedSeq(
      "property_1" as "devil",
      "camel_case_property" as "other",
      "column" as "eye")
    val map = mapper.columnMapForReading(table2, selectedColumns)
    assertEquals(selectedColumns, map.constructor)
  }

}
