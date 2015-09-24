package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.junit.Assert._
import org.junit.Test

import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.RandomPartitionerTokenFactory
import com.datastax.spark.connector.rdd.partitioner.dht.{BigIntToken, TokenFactory}

class RandomPartitionerTokenRangeSplitterTest {

  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[BigInt, BigIntToken]

  private def assertNoHoles(tokenRanges: Seq[TokenRange]) {
    for (Seq(range1, range2) <- tokenRanges.sliding(2))
      assertEquals(range1.end, range2.start)
  }

  private def assertSimilarSize(tokenRanges: Seq[TokenRange]): Unit = {
    val sizes = tokenRanges.map(r => RandomPartitionerTokenFactory.distance(r.start, r.end)).toVector
    val maxSize = sizes.max.toDouble
    val minSize = sizes.min.toDouble
    assertTrue(s"maxSize / minSize = ${maxSize / minSize} > 1.01", maxSize / minSize <= 1.01)
  }

  @Test
  def testSplit() {
    val dataSize = 1000
    val node = InetAddress.getLocalHost
    val splitter = new RandomPartitionerTokenRangeSplitter(dataSize)
    val rangeLeft = BigInt("0")
    val rangeRight = BigInt("0")
    val range = new TokenRange(BigIntToken(rangeLeft), BigIntToken(rangeRight), Set(node), dataSize)
    val out = splitter.split(range, 100)

    assertEquals(10, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
    assertTrue(out.forall(_.replicas == Set(node)))
    assertNoHoles(out)
    assertSimilarSize(out)
  }

  @Test
  def testNoSplit() {
    val splitter = new RandomPartitionerTokenRangeSplitter(1000)
    val rangeLeft = BigInt("0")
    val rangeRight = BigInt("100")
    val range = new TokenRange(BigIntToken(rangeLeft), BigIntToken(rangeRight), Set.empty, 0)
    val out = splitter.split(range, 500)

    // range is too small to contain 500 rows
    assertEquals(1, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
  }

  @Test
  def testZeroRows() {
    val splitter = new RandomPartitionerTokenRangeSplitter(0)
    val rangeLeft = BigInt("0")
    val rangeRight = BigInt("100")
    val range = new TokenRange(BigIntToken(rangeLeft), BigIntToken(rangeRight), Set.empty, 0)
    val out = splitter.split(range, 500)
    assertEquals(1, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
  }

  @Test
  def testWrapAround() {
    val dataSize = 2000
    val splitter = new RandomPartitionerTokenRangeSplitter(dataSize)
    val totalTokenCount = RandomPartitionerTokenFactory.totalTokenCount
    val rangeLeft = RandomPartitionerTokenFactory.maxToken.value - totalTokenCount / 4
    val rangeRight = RandomPartitionerTokenFactory.minToken.value + totalTokenCount / 4
    val range = new TokenRange(
      new BigIntToken(rangeLeft),
      new BigIntToken(rangeRight),
      Set.empty,
      dataSize / 2)
    val out = splitter.split(range, 100)
    assertEquals(10, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
    assertNoHoles(out)
    assertSimilarSize(out)
  }

}
