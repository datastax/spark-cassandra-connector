package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnName
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{VarCharType, IntType}
import org.junit.Assert._
import org.junit.Test

case class DefaultColumnMapperTestClass1(property1: String, camelCaseProperty: Int, UpperCaseColumn: Int)

class DefaultColumnMapperTestClass2(var property1: String, var camelCaseProperty: Int, var UpperCaseColumn: Int)

case class ClassWithWeirdProps(devil: String, cat: Int, eye: Double)

class DefaultColumnMapperTest {

  private val c1 = ColumnDef("property_1", PartitionKeyColumn, IntType)
  private val c2 = ColumnDef("camel_case_property", ClusteringColumn(0), IntType)
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
  def testImplicit() {
    val mapper = implicitly[ColumnMapper[DefaultColumnMapperTestClass1]]
    assertTrue(mapper.isInstanceOf[DefaultColumnMapper[_]])
  }

  @Test
  def testNewTableForCaseClass(): Unit = {
    val mapper = implicitly[ColumnMapper[DefaultColumnMapperTestClass1]]
    val table = mapper.newTable("keyspace", "table")

    assertEquals("keyspace", table.keyspaceName)
    assertEquals("table", table.tableName)
    assertEquals(3, table.columns.size)
    assertEquals(1, table.partitionKey.size)
    assertEquals("property_1", table.columns(0).columnName)
    assertEquals(VarCharType, table.columns(0).columnType)
    assertEquals("camel_case_property", table.columns(1).columnName)
    assertEquals(IntType, table.columns(1).columnType)
    assertEquals("upper_case_column", table.columns(2).columnName)
    assertEquals(IntType, table.columns(2).columnType)
  }

  @Test
  def testNewTableForClassWithVars(): Unit = {
    val mapper = implicitly[ColumnMapper[DefaultColumnMapperTestClass2]]
    val table = mapper.newTable("keyspace", "table")

    assertEquals("keyspace", table.keyspaceName)
    assertEquals("table", table.tableName)
    assertEquals(3, table.columns.size)
    assertEquals(1, table.partitionKey.size)
    assertEquals("property_1", table.columns(0).columnName)
    assertEquals(VarCharType, table.columns(0).columnType)
    assertEquals("camel_case_property", table.columns(1).columnName)
    assertEquals(IntType, table.columns(1).columnType)
    assertEquals("upper_case_column", table.columns(2).columnName)
    assertEquals(IntType, table.columns(2).columnType)
  }

  class Foo
  case class ClassWithUnsupportedPropertyType(property1: Foo, property2: Int)

  @Test
  def testNewTableForClassWithUnsupportedPropertyType(): Unit = {
    val mapper = implicitly[ColumnMapper[ClassWithUnsupportedPropertyType]]
    val table = mapper.newTable("keyspace", "table")

    assertEquals("keyspace", table.keyspaceName)
    assertEquals("table", table.tableName)
    assertEquals(1, table.columns.size)
    assertEquals(1, table.partitionKey.size)
    assertEquals("property_2", table.columns(0).columnName)
    assertEquals(IntType, table.columns(0).columnType)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNewTableForEmptyClass(): Unit = {
    val mapper = implicitly[ColumnMapper[Foo]]     // should fail, because there are no useful properties
    mapper.newTable("keyspace", "table")
  }

  @Test
  def testWorkWithAliases() {
    val mapper = new DefaultColumnMapper[ClassWithWeirdProps]()
    val map = mapper.columnMap(tableDef, Map("devil" -> "property_1", "cat" -> "camel_case_property", "eye" -> "column"))
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c4.columnName))
    assertEquals(expectedConstructor, map.constructor)
  }

  @Test
  def testWorkWithAliasesAndHonorOverrides() {
    val mapper = new DefaultColumnMapper[ClassWithWeirdProps](Map("cat" -> "UpperCaseColumn"))
    val map = mapper.columnMap(tableDef, Map("devil" -> "property_1", "cat" -> "camel_case_property", "eye" -> "column"))
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c3.columnName),
      ColumnName(c4.columnName))
    assertEquals(expectedConstructor, map.constructor)
  }

}