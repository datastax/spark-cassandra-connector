package com.datastax.driver.spark.writer

import java.util.Date

import org.junit.Assert._
import org.junit.Test

class ObjectSizeEstimatorTest {

  @Test
  def testFunctionality() {
    val size0 = ObjectSizeEstimator.measureSerializedSize(Array(1))
    val size1 = ObjectSizeEstimator.measureSerializedSize(Array(1, 2))
    val size2 = ObjectSizeEstimator.measureSerializedSize(Array(1, 2, "abc", List("item1", "item2"), new Date()))
    assertTrue(size0 > 16)
    assertTrue(size1 > size0)
    assertTrue(size2 > size1)
  }
}
