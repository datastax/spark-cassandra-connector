package com.datastax.driver.spark.rdd.reader

import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.util.SerializationUtil
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
