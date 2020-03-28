package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector.cql.{RegularColumn, ColumnDef, PartitionKeyColumn, TableDef}
import org.apache.commons.lang3.SerializationUtils
import org.junit.Test

import com.datastax.spark.connector.types.{BigIntType, IntType, VarCharType}

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowReaderTest {

  private val a = ColumnDef("a", PartitionKeyColumn, VarCharType)
  private val b = ColumnDef("b", RegularColumn, IntType)
  private val c = ColumnDef("c", RegularColumn, BigIntType)
  private val table = TableDef("test", "table", Seq(a), Nil, Seq(b, c))

  @Test
  def testSerialize() {
    val reader = new ClassBasedRowReader[TestClass](table, table.columnRefs)
    SerializationUtils.roundtrip(reader)
  }

}
