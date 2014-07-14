package com.datastax.spark.connector.writer

import com.datastax.spark.connector.types.TypeConverter.{StringConverter, OptionToNullConverter, IntConverter}

import org.junit.Assert._
import org.junit.Test

class ConvertingPropertyExtractorTest {

  class TestClass(val field1: String, val field2: Option[Int])

  private def createExtractor: ConvertingPropertyExtractor[TestClass] = {
    new ConvertingPropertyExtractor[TestClass](
      classOf[TestClass], Seq(
        ("field1", IntConverter),
        ("field2", new OptionToNullConverter(StringConverter))))
  }

  @Test
  def testExtraction() {
    val obj = new TestClass("123", Some(5))
    val extractor = createExtractor
    val data = extractor.extract(obj)
    assertNotNull(data)
    assertEquals(2, data.length)
    assertEquals(123, data(0))
    assertEquals("5", data(1))
  }

  @Test
  def testExtractionNoAlloc() {
    val obj = new TestClass("123", Some(5))
    val extractor = createExtractor
    val data = Array.ofDim[AnyRef](extractor.propertyNames.size)
    extractor.extract(obj, data)
    assertEquals(123, data(0))
    assertEquals("5", data(1))

  }
}
