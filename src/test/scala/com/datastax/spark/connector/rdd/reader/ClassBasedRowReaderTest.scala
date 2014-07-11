package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.util.SerializationUtil
import org.junit.Test

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowReaderTest {

  private val tableDef = TableDef("test", "table", Nil, Nil, Nil)

  @Test
  def testSerialize() {
    val reader = new ClassBasedRowReader[TestClass](tableDef)
    SerializationUtil.serializeAndDeserialize(reader)
  }

}
