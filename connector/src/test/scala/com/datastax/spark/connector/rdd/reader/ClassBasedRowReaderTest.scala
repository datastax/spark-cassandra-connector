package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector.mapper.{ColumnDescriptor, TableDescriptor}
import org.apache.commons.lang3.SerializationUtils
import org.junit.Test
import com.datastax.spark.connector.types.{BigIntType, IntType, VarCharType}

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowReaderTest {

  private val a = ColumnDescriptor.partitionKey("a", VarCharType)
  private val b = ColumnDescriptor.regularColumn("b", IntType)
  private val c = ColumnDescriptor.regularColumn("c", BigIntType)
  private val table = TableDescriptor("test", "table", Seq(a, b, c))

  @Test
  def testSerialize() {
    val reader = new ClassBasedRowReader[TestClass](table, table.columnRefs)
    SerializationUtils.roundtrip(reader)
  }
}
