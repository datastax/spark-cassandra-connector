package com.datastax.spark.connector.types

import com.datastax.spark.connector.util.SerializationUtil
import org.junit.Assert._
import org.junit.Test

class CanBuildFromTest {

  @Test
  def testBuild() {
    val bf = CanBuildFrom.setCanBuildFrom[Int]
    val builder = bf.apply()
    builder += 1
    builder += 2
    builder += 3
    assertEquals(Set(1,2,3), builder.result())
  }

  @Test
  def testSerializeAndBuild() {
    val bf = CanBuildFrom.setCanBuildFrom[Int]
    val bf2 = SerializationUtil.serializeAndDeserialize(bf)
    val builder = bf2.apply()
    builder += 1
    builder += 2
    builder += 3
    assertEquals(Set(1,2,3), builder.result())
  }

  @Test
  def testSerializeAndBuildWithOrdering() {
    val bf = CanBuildFrom.treeSetCanBuildFrom[Int]
    val bf2 = SerializationUtil.serializeAndDeserialize(bf)
    val builder = bf2.apply()
    builder += 1
    builder += 2
    builder += 3
    assertEquals(Set(1,2,3), builder.result())
  }


}
