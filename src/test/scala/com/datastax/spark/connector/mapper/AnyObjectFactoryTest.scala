package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.rdd.reader.AnyObjectFactory
import com.datastax.spark.connector.util.SerializationUtil
import org.junit.Assert._
import org.junit.Test

class TopLevel(val arg1: String, val arg2: Int)

class SerializableFactoryTest {

  @Test
  def testObjectCreation() {
    val factory = new AnyObjectFactory[TopLevel]
    val obj = factory.newInstance("test", 1.asInstanceOf[AnyRef])
    assertNotNull(obj)
    assertEquals("test", obj.arg1)
    assertEquals(1, obj.arg2)
  }

  @Test
  def testSerialize() {
    val factory = SerializationUtil.serializeAndDeserialize(new AnyObjectFactory[TopLevel])
    val obj = factory.newInstance("test", 1.asInstanceOf[AnyRef])
    assertNotNull(obj)
    assertEquals("test", obj.arg1)
    assertEquals(1, obj.arg2)
  }

}
