package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector.cql.{DefaultColumnDef, DefaultTableDef, PartitionKeyColumn, RegularColumn}
import org.apache.commons.lang3.SerializationUtils
import org.junit.Test
import com.datastax.spark.connector.types.{BigIntType, IntType, VarCharType}

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowReaderTest {

  private val a = DefaultColumnDef("a", PartitionKeyColumn, VarCharType)
  private val b = DefaultColumnDef("b", RegularColumn, IntType)
  private val c = DefaultColumnDef("c", RegularColumn, BigIntType)
  private val table = DefaultTableDef("test", "table", Seq(a), Nil, Seq(b, c))

  @Test
  def testSerialize() {
    val reader = new ClassBasedRowReader[TestClass](table, table.columnRefs)
    SerializationUtils.roundtrip(reader)
  }

}
