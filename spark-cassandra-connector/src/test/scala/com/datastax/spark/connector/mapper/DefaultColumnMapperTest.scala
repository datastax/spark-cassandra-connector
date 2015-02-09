package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnName
import com.datastax.spark.connector.cql.{RegularColumn, TableDef, ColumnDef}
import com.datastax.spark.connector.types.IntType
import org.apache.commons.lang3.SerializationUtils
import org.junit.Assert._
import org.junit.Test

case class DefaultColumnMapperTestClass1(property1: String, camelCaseProperty: Int, UpperCaseColumn: Int)

class DefaultColumnMapperTestClass2(var property1: String, var camelCaseProperty: Int, var UpperCaseColumn: Int)

class DefaultColumnMapperTest {

  private val c1 = ColumnDef("property_1", RegularColumn, IntType)
  private val c2 = ColumnDef("camel_case_property", RegularColumn, IntType)
  private val c3 = ColumnDef("UpperCaseColumn", RegularColumn, IntType)
  private val c4 = ColumnDef("column", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3, c4))

  @Test
  def testGetters1() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1].columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(ColumnName(c1.columnName), getters("property1"))
    assertEquals(ColumnName(c2.columnName), getters("camelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("UpperCaseColumn"))
  }

  @Test
  def testGetters2() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2].columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(ColumnName(c1.columnName), getters("property1"))
    assertEquals(ColumnName(c2.columnName), getters("camelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("UpperCaseColumn"))
  }

  @Test
  def testSetters1() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1].columnMap(tableDef)
    assertTrue(columnMap.setters.isEmpty)
  }

  @Test
  def testSetters2() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2].columnMap(tableDef)
    val setters = columnMap.setters
    assertEquals(ColumnName(c1.columnName), setters("property1_$eq"))
    assertEquals(ColumnName(c2.columnName), setters("camelCaseProperty_$eq"))
    assertEquals(ColumnName(c3.columnName), setters("UpperCaseColumn_$eq"))
  }

  @Test
  def testConstructorParams1() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1].columnMap(tableDef)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    assertEquals(expectedConstructor, columnMap.constructor)
  }

  @Test
  def testConstructorParams2() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2].columnMap(tableDef)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    assertEquals(expectedConstructor, columnMap.constructor)
  }

  @Test
  def columnNameOverrideGetters() {
    val nameOverride: Map[String, String] = Map("property1" -> c4.columnName)
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1](nameOverride).columnMap(tableDef)
    val getters = columnMap.getters
    assertEquals(ColumnName(c4.columnName), getters("property1"))
    assertEquals(ColumnName(c2.columnName), getters("camelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("UpperCaseColumn"))
  }

  @Test
  def columnNameOverrideSetters() {
    val nameOverride: Map[String, String] = Map("property1" -> c4.columnName)
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2](nameOverride).columnMap(tableDef)
    val setters = columnMap.setters
    assertEquals(ColumnName(c4.columnName), setters("property1_$eq"))
    assertEquals(ColumnName(c2.columnName), setters("camelCaseProperty_$eq"))
    assertEquals(ColumnName(c3.columnName), setters("UpperCaseColumn_$eq"))
  }

  @Test
  def columnNameOverrideConstructor() {
    val nameOverride: Map[String, String] = Map("property1" -> "column")
    val mapper = new DefaultColumnMapper[DefaultColumnMapperTestClass1](nameOverride).columnMap(tableDef)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c4.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    assertEquals(expectedConstructor, mapper.constructor)
  }

  @Test
  def testSerialize() {
    val mapper = new DefaultColumnMapper[DefaultColumnMapperTestClass1]
    SerializationUtils.roundtrip(mapper)
  }

  @Test
  def testImplicit() {
    val mapper = implicitly[ColumnMapper[DefaultColumnMapperTestClass1]]
    assertTrue(mapper.isInstanceOf[DefaultColumnMapper[_]])
  }

}
