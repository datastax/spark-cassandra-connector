package com.datastax.spark.connector.writer

import java.nio.ByteBuffer
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

  @Test
  def testByteBuffers() {
    val buffer = ByteBuffer.allocate(100)
    val size0 = ObjectSizeEstimator.measureSerializedSize(Array(buffer))
    val size1 = ObjectSizeEstimator.measureSerializedSize(Array(List(buffer)))
    val size2 = ObjectSizeEstimator.measureSerializedSize(Array(Set(buffer)))
    val size3 = ObjectSizeEstimator.measureSerializedSize(Array(Map(1 -> buffer)))
    assertTrue(size0 > 100)
    assertTrue(size1 > 100)
    assertTrue(size2 > 100)
    assertTrue(size3 > 100)
  }
}
