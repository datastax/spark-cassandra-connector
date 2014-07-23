package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.cql.{TableDef, RegularColumn, ColumnDef}
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

object JavaBeanColumnMapperTestClass {
  implicit object Mapper extends JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]()
}

class JavaBeanColumnMapperTest {

  private val c1 = ColumnDef("test", "table", "property_1", RegularColumn, IntType)
  private val c2 = ColumnDef("test", "table", "camel_case_property", RegularColumn, IntType)
  private val c3 = ColumnDef("test", "table", "flagged", RegularColumn, IntType)
  private val c4 = ColumnDef("test", "table", "marked", RegularColumn, IntType)
  private val c5 = ColumnDef("test", "table", "column", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3, c4, c5))

  @Test
  def testGetters() {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass].columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(NamedColumnRef(c1.columnName), getters("getProperty1"))
    assertEquals(NamedColumnRef(c2.columnName), getters("getCamelCaseProperty"))
    assertEquals(NamedColumnRef(c3.columnName), getters("isFlagged"))
  }

  @Test
  def testSetters() {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass].columnMap(tableDef)
    val setters = columnMap.setters
    assertEquals(NamedColumnRef(c1.columnName), setters("setProperty1"))
    assertEquals(NamedColumnRef(c2.columnName), setters("setCamelCaseProperty"))
    assertEquals(NamedColumnRef(c3.columnName), setters("setFlagged"))
  }

  @Test
  def testColumnNameOverrideGetters() {
    val columnNameOverrides: Map[String, String] = Map("property1" -> c5.columnName, "flagged" -> c4.columnName)
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass](columnNameOverrides).columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(NamedColumnRef(c5.columnName), getters("getProperty1"))
    assertEquals(NamedColumnRef(c2.columnName), getters("getCamelCaseProperty"))
    assertEquals(NamedColumnRef(c4.columnName), getters("isFlagged"))
  }

  @Test
  def testColumnNameOverrideSetters() {
    val columnNameOverrides: Map[String, String] = Map("property1" -> c5.columnName, "flagged" -> c4.columnName)
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass](columnNameOverrides).columnMap(tableDef)
    val setters = columnMap.setters
    assertEquals(NamedColumnRef(c5.columnName), setters("setProperty1"))
    assertEquals(NamedColumnRef(c2.columnName), setters("setCamelCaseProperty"))
    assertEquals(NamedColumnRef(c4.columnName), setters("setFlagged"))
  }

  @Test
  def testSerializeColumnMapper() {
    val mapper = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]
    SerializationUtils.roundtrip(mapper)
  }

  @Test
  def testSerializeColumnMap() {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass].columnMap(tableDef)
    SerializationUtils.roundtrip(columnMap)
  }

  @Test
  def testImplicit() {
    val mapper = implicitly[ColumnMapper[JavaBeanColumnMapperTestClass]]
    assertTrue(mapper.isInstanceOf[JavaBeanColumnMapper[_]])
  }

}
