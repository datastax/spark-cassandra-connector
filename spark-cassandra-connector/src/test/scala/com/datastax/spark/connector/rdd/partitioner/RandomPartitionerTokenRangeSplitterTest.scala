package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.junit.Assert._
import org.junit.Test

import com.datastax.spark.connector.rdd.partitioner.dht.{BigIntToken, TokenFactory}

class RandomPartitionerTokenRangeSplitterTest {

  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[BigInt, BigIntToken]

  private def assertNoHoles(tokenRanges: Seq[TokenRange]) {
    for (Seq(range1, range2) <- tokenRanges.sliding(2))
      assertEquals(range1.end, range2.start)
  }

  @Test
  def testSplit() {
    val node = InetAddress.getLocalHost
    val splitter = new RandomPartitionerTokenRangeSplitter(2.0)
    val rangeLeft = BigInt("0")
    val rangeRight = BigInt("100")
    val range = new TokenRange(
      new BigIntToken(rangeLeft),
      new BigIntToken(rangeRight), Set(node), None)
    val out = splitter.split(range, 20)

    // 2 rows per token on average; to so 10 tokens = 20 rows; therefore 10 splits
    assertEquals(10, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
    assertTrue(out.forall(_.replicas == Set(node)))
    assertNoHoles(out)
  }

  @Test
  def testNoSplit() {
    val splitter = new RandomPartitionerTokenRangeSplitter(2.0)
    val rangeLeft = BigInt("0")
    val rangeRight = BigInt("100")
    val range = new TokenRange(
      new BigIntToken(rangeLeft),
      new BigIntToken(rangeRight), Set.empty, None)
    val out = splitter.split(range, 500)

    // range is too small to contain 500 rows
    assertEquals(1, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
  }

  @Test
  def testZeroRows() {
    val splitter = new RandomPartitionerTokenRangeSplitter(0.0)
    val rangeLeft = BigInt("0")
    val rangeRight = BigInt("100")
    val range = new TokenRange(
      new BigIntToken(rangeLeft),
      new BigIntToken(rangeRight), Set.empty, None)
    val out = splitter.split(range, 500)
    assertEquals(1, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
  }

  @Test
  def testWrapAround() {
    val splitter = new RandomPartitionerTokenRangeSplitter(2.0)
    val rangeLeft = TokenFactory.RandomPartitionerTokenFactory.maxToken.value - 100
    val rangeRight = BigInt("100")
    val range = new TokenRange(
      new BigIntToken(rangeLeft),
      new BigIntToken(rangeRight), Set.empty, None)
    val out = splitter.split(range, 20)
    assertEquals(20, out.size)
    assertEquals(rangeLeft, out.head.start.value)
    assertEquals(rangeRight, out.last.end.value)
    assertNoHoles(out)
  }

}
