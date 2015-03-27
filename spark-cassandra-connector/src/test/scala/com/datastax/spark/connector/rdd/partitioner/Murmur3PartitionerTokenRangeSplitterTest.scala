package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.junit.Assert._
import org.junit.Test

import com.datastax.spark.connector.rdd.partitioner.dht.LongToken
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory

class Murmur3PartitionerTokenRangeSplitterTest {

  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[Long, LongToken]

  private def assertNoHoles(tokenRanges: Seq[TokenRange]) {
    for (Seq(range1, range2) <- tokenRanges.sliding(2))
      assertEquals(range1.end, range2.start)
  }

  @Test
  def testSplit() {
    val node = InetAddress.getLocalHost
    val splitter = new Murmur3PartitionerTokenRangeSplitter(2.0)
    val range = new TokenRange(
      new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(0),
      new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(100),
      Set(node), None)
    val out = splitter.split(range, 20)

    // 2 rows per token on average; to so 10 tokens = 20 rows; therefore 10 splits
    assertEquals(10, out.size)
    assertEquals(0L, out.head.start.value)
    assertEquals(100L, out.last.end.value)
    assertTrue(out.forall(s => s.end.value - s.start.value == 10))
    assertTrue(out.forall(_.replicas == Set(node)))
    assertNoHoles(out)
  }

  @Test
  def testNoSplit() {
    val splitter = new Murmur3PartitionerTokenRangeSplitter(2.0)
    val range = new TokenRange(
      new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(0), new LongToken(100), Set.empty, None)
    val out = splitter.split(range, 500)

    // range is too small to contain 500 rows
    assertEquals(1, out.size)
    assertEquals(0L, out.head.start.value)
    assertEquals(100L, out.last.end.value)
  }

  @Test
  def testZeroRows() {
    val splitter = new Murmur3PartitionerTokenRangeSplitter(0.0)
    val range = new TokenRange(
      new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(0), new LongToken(100), Set.empty, None)
    val out = splitter.split(range, 500)
    assertEquals(1, out.size)
    assertEquals(0L, out.head.start.value)
    assertEquals(100L, out.last.end.value)
  }

  @Test
  def testWrapAround() {
    val splitter = new Murmur3PartitionerTokenRangeSplitter(2.0)
    val maxValue = Murmur3TokenFactory.maxToken.value
    val minValue = Murmur3TokenFactory.minToken.value
    val range = new TokenRange(
      new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(maxValue - 100),
      new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(minValue + 100), Set.empty, None)
    val splits = splitter.split(range, 20)
    assertEquals(20, splits.size)
    assertEquals(maxValue - 100, splits.head.start.value)
    assertEquals(minValue + 100, splits.last.end.value)
    assertNoHoles(splits)
  }
}
