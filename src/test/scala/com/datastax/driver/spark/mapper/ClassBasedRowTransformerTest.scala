package com.datastax.driver.spark.mapper

import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.util.SerializationUtil
import org.junit.Assert._
import org.junit.Test

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowTransformerTest {

  private val tableDef = TableDef("test", "table", Nil, Nil, Nil)

  private def testTransformer(transformer: ClassBasedRowTransformer[TestClass]) {
    val row = Array[AnyRef]("text", "10", "22222")
    assertEquals(Some(3), transformer.columnNames.map(_.size))
    assertEquals(TestClass("text", 10, Some(22222L)), transformer.transform(row))
  }

  @Test
  def testTransform() {
    val transformer = new ClassBasedRowTransformer[TestClass](tableDef)
    testTransformer(transformer)
  }

  @Test
  def testSerialize() {
    val transformer = new ClassBasedRowTransformer[TestClass](tableDef)
    val transformer2 = SerializationUtil.serializeAndDeserialize(transformer)
    testTransformer(transformer2)
  }

}
