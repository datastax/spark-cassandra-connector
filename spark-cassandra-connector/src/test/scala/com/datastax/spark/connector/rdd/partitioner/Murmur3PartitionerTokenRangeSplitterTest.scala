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

  private def assertSimilarSize(tokenRanges: Seq[TokenRange]): Unit = {
    val sizes = tokenRanges.map(r => Murmur3TokenFactory.distance(r.start, r.end)).toVector
    val maxSize = sizes.max.toDouble
    val minSize = sizes.min.toDouble
    assertTrue(s"maxSize / minSize = ${maxSize / minSize} > 1.01", maxSize / minSize <= 1.01)
  }

  @Test
  def testSplit() {
    val node = InetAddress.getLocalHost
    val dataSize = 1000
    val splitter = new Murmur3PartitionerTokenRangeSplitter(dataSize)
    val range = new TokenRange(LongToken(0), LongToken(0), Set(node), dataSize)
    val out = splitter.split(range, 100)

    assertEquals(10, out.size)
    assertEquals(0L, out.head.start.value)
    assertEquals(0L, out.last.end.value)
    assertTrue(out.forall(s => s.end.value != s.start.value))
    assertTrue(out.forall(_.replicas == Set(node)))
    assertNoHoles(out)
    assertSimilarSize(out)
  }


  @Test
  def testNoSplit() {
    val splitter = new Murmur3PartitionerTokenRangeSplitter(1000)
    val range = new TokenRange(LongToken(0), new LongToken(100), Set.empty, 0)
    val out = splitter.split(range, 500)

    // range is too small to contain 500 units
    assertEquals(1, out.size)
    assertEquals(0L, out.head.start.value)
    assertEquals(100L, out.last.end.value)
  }

  @Test
  def testZeroRows() {
    val dataSize = 0
    val splitter = new Murmur3PartitionerTokenRangeSplitter(dataSize)
    val range = new TokenRange(LongToken(0), LongToken(100), Set.empty, dataSize)
    val out = splitter.split(range, 500)
    assertEquals(1, out.size)
    assertEquals(0L, out.head.start.value)
    assertEquals(100L, out.last.end.value)
  }

  @Test
  def testWrapAround() {
    val dataSize = 2000
    val splitter = new Murmur3PartitionerTokenRangeSplitter(dataSize)
    val start = Murmur3TokenFactory.maxToken.value - Long.MaxValue / 2
    val end = Murmur3TokenFactory.minToken.value + Long.MaxValue / 2
    val range = new TokenRange(LongToken(start), LongToken(end), Set.empty, dataSize / 2)
    val splits = splitter.split(range, 100)

    // range is half of the ring; 2000 * 0.5 / 100 = 10
    assertEquals(10, splits.size)
    assertEquals(start, splits.head.start.value)
    assertEquals(end, splits.last.end.value)
    assertNoHoles(splits)
    assertSimilarSize(splits)
  }
}
