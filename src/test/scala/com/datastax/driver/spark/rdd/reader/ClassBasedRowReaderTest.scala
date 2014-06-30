package com.datastax.driver.spark.rdd.reader

import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.util.SerializationUtil
import org.junit.Assert._
import org.junit.Test

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowReaderTest {

  private val tableDef = TableDef("test", "table", Nil, Nil, Nil)

  private def testReader(transformer: ClassBasedRowReader[TestClass]) {
    val row = Array[AnyRef]("text", "10", "22222")
    assertEquals(Some(3), transformer.columnNames.map(_.size))
    assertEquals(TestClass("text", 10, Some(22222L)), transformer.transform(row))
  }

  @Test
  def testTransform() {
    val reader = new ClassBasedRowReader[TestClass](tableDef)
    testReader(reader)
  }

  @Test
  def testSerialize() {
    val transformer = new ClassBasedRowReader[TestClass](tableDef)
    val transformer2 = SerializationUtil.serializeAndDeserialize(transformer)
    testReader(transformer2)
  }

}
