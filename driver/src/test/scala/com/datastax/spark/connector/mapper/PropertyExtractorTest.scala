package com.datastax.spark.connector.mapper

import org.junit.Assert._
import org.junit.Test

class PropertyExtractorTest {

  class TestClass(val field1: String, val field2: Int)

  @Test
  def testSimpleExtraction() {
    val testObject = new TestClass("a", 1)
    val propertyExtractor = new PropertyExtractor(classOf[TestClass], Seq("field1", "field2"))
    val result = propertyExtractor.extract(testObject)
    assertEquals(2, result.length)
    assertEquals("a", result(0))
    assertEquals(1, result(1))
  }

  @Test
  def testAvailableProperties() {
    val triedProperties = Seq("field1", "foo", "bar")
    val availableProperties = PropertyExtractor.availablePropertyNames(classOf[TestClass], triedProperties)
    assertEquals(Seq("field1"), availableProperties)
  }

  @Test(expected = classOf[NoSuchMethodException])
  def testWrongPropertyName() {
    val testObject = new TestClass("a", 1)
    val propertyExtractor = new PropertyExtractor(classOf[TestClass], Seq("foo"))
    propertyExtractor.extract(testObject)
  }

}
