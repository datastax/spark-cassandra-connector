package com.datastax.spark.connector.mapper

import com.datastax.spark.connector._
import com.datastax.spark.connector.{AllColumns, ColumnName}
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
  private val table1 = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3))

  private val c4 = ColumnDef("column", PartitionKeyColumn, IntType)
  private val table2 = TableDef("test", "table", Seq(c4), Seq(c2), Seq(c3))

  @Test
  def testGetters1() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1]()
      .columnMapForWriting(table1, table1.columnRefs)
    val getters = columnMap.getters
    assertEquals(ColumnName(c1.columnName), getters("property1"))
    assertEquals(ColumnName(c2.columnName), getters("camelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("UpperCaseColumn"))
  }

  @Test
  def testGetters2() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2]()
      .columnMapForWriting(table1, table1.columnRefs)
    val getters = columnMap.getters
    assertEquals(ColumnName(c1.columnName), getters("property1"))
    assertEquals(ColumnName(c2.columnName), getters("camelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("UpperCaseColumn"))
  }

  @Test
  def testSetters1() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1]()
      .columnMapForReading(table1, table1.columnRefs)
    assertTrue(columnMap.setters.isEmpty)
  }

  @Test
  def testSetters2() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2]()
      .columnMapForReading(table1, table1.columnRefs)
    val setters = columnMap.setters
    assertEquals(ColumnName(c1.columnName), setters("property1_$eq"))
    assertEquals(ColumnName(c2.columnName), setters("camelCaseProperty_$eq"))
    assertEquals(ColumnName(c3.columnName), setters("UpperCaseColumn_$eq"))
  }

  @Test
  def testConstructorParams1() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1]()
      .columnMapForReading(table1, table1.columnRefs)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    assertEquals(expectedConstructor, columnMap.constructor)
  }

  @Test
  def testConstructorParams2() {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2]().columnMapForReading(
      table1, table1.columnRefs)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    assertEquals(expectedConstructor, columnMap.constructor)
  }

  @Test
  def columnNameOverrideGetters() {
    val nameOverride: Map[String, String] = Map("property1" -> c4.columnName)
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1](nameOverride)
      .columnMapForWriting(table2, table2.columnRefs)
    val getters = columnMap.getters
    assertEquals(ColumnName(c4.columnName), getters("property1"))
    assertEquals(ColumnName(c2.columnName), getters("camelCaseProperty"))
    assertEquals(ColumnName(c3.columnName), getters("UpperCaseColumn"))
  }

  @Test
  def columnNameOverrideSetters() {
    val nameOverride: Map[String, String] = Map("property1" -> c4.columnName)
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2](nameOverride)
      .columnMapForReading(table2, table2.columnRefs)
    val setters = columnMap.setters
    assertEquals(ColumnName(c4.columnName), setters("property1_$eq"))
    assertEquals(ColumnName(c2.columnName), setters("camelCaseProperty_$eq"))
    assertEquals(ColumnName(c3.columnName), setters("UpperCaseColumn_$eq"))
  }

  @Test
  def columnNameOverrideConstructor() {
    val nameOverride: Map[String, String] = Map("property1" -> "column")
    val mapper = new DefaultColumnMapper[DefaultColumnMapperTestClass1](nameOverride)
      .columnMapForReading(table2, table2.columnRefs)
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
    val selectedColumns = IndexedSeq(
      "property_1" as "devil",
      "camel_case_property" as "cat",
      "column" as "eye")
    val map = mapper.columnMapForReading(table2, selectedColumns)
    assertEquals(selectedColumns, map.constructor)
  }

  @Test
  def testWorkWithAliasesAndHonorOverrides() {
    val mapper = new DefaultColumnMapper[ClassWithWeirdProps](Map("cat" -> "cat2"))
    val selectedColumns = IndexedSeq(
      "property_1" as "devil",
      "camel_case_property" as "cat2",
      "UpperCaseColumn" as "eye")
    val map = mapper.columnMapForReading(table1, selectedColumns)
    assertEquals(selectedColumns, map.constructor)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNotEnoughColumnsSelectedForReading(): Unit = {
    new DefaultColumnMapper[DefaultColumnMapperTestClass1]()
      .columnMapForReading(table1, table1.columnRefs.tail)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNotEnoughPropertiesForWriting(): Unit = {
    new DefaultColumnMapper[DefaultColumnMapperTestClass1]()
      .columnMapForWriting(table1, table1.columnRefs :+ ColumnName("missingColumn"))
  }

}