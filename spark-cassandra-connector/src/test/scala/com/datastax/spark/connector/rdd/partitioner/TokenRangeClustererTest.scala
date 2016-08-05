package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory.{maxToken, minToken}
import com.datastax.spark.connector.rdd.partitioner.dht.{LongToken, TokenRange}
import org.junit.Assert._
import org.junit.Test

class TokenRangeClustererTest {

  val node1 = InetAddress.getByName("192.168.123.1")
  val node2 = InetAddress.getByName("192.168.123.2")
  val node3 = InetAddress.getByName("192.168.123.3")
  val node4 = InetAddress.getByName("192.168.123.4")
  val node5 = InetAddress.getByName("192.168.123.5")

  private def tokenRange(start: Long, end: Long, nodes: Set[InetAddress]): TokenRange[Long, LongToken] =
    new TokenRange[Long, LongToken](new LongToken(start), new LongToken(end), nodes, Murmur3TokenFactory)

  private implicit def tokenToLong(token: LongToken): Long = token.value

  @Test
  def testEmpty() {
    val trc = new TokenRangeClusterer(10)
    val groups = trc.group(Seq.empty)
    assertEquals(0, groups.size)
  }

  @Test
  def testTrivialClustering() {
    val tr1 = tokenRange(start = 0, end = 10, nodes = Set(node1))
    val tr2 = tokenRange(start = 10, end = 20, nodes = Set(node1))
    val trc = new TokenRangeClusterer[Long, LongToken](1)
    val groups = trc.group(Seq(tr1, tr2))
    assertEquals(1, groups.size)
    assertEquals(Set(tr1, tr2), groups.head.toSet)
  }

  @Test
  def testSplitByHost() {
    val tr1 = tokenRange(start = 0, end = 10, nodes = Set(node1))
    val tr2 = tokenRange(start = 10, end = 20, nodes = Set(node1))
    val tr3 = tokenRange(start = 20, end = 30, nodes = Set(node2))
    val tr4 = tokenRange(start = 30, end = 40, nodes = Set(node2))

    val trc = new TokenRangeClusterer[Long, LongToken](1)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testSplitByCount() {
    val tr1 = tokenRange(start = minToken, end = minToken / 2, Set(node1))
    val tr2 = tokenRange(start = minToken / 2, end = 0, Set(node1))
    val tr3 = tokenRange(start = 0, end = maxToken / 2, Set(node1))
    val tr4 = tokenRange(start = maxToken / 2, end = maxToken, Set(node1))

    val trc = new TokenRangeClusterer[Long, LongToken](2)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testMultipleEndpoints() {
    val tr1 = tokenRange(start = 0, end = 10, nodes = Set(node2, node1, node3))
    val tr2 = tokenRange(start = 10, end = 20, nodes = Set(node1, node3, node4))
    val tr3 = tokenRange(start = 20, end = 30, nodes = Set(node3, node1, node5))
    val tr4 = tokenRange(start = 30, end = 40, nodes = Set(node3, node1, node4))
    val trc = new TokenRangeClusterer[Long, LongToken](1)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4))
    assertEquals(1, groups.size)
    assertEquals(4, groups.head.size)
    assertFalse(groups.head.map(_.replicas).reduce(_ intersect _).isEmpty)
  }

  @Test
  def testMaxGroupSize() {
    val tr1 = tokenRange(start = 0, end = 10, nodes = Set(node1, node2, node3))
    val tr2 = tokenRange(start = 10, end = 20, nodes = Set(node1, node2, node3))
    val tr3 = tokenRange(start = 20, end = 30, nodes = Set(node1, node2, node3))
    val trc = new TokenRangeClusterer[Long, LongToken](groupCount = 1, maxGroupSize = 1)
    val groups = trc.group(Seq(tr1, tr2, tr3))
    assertEquals(3, groups.size)
  }
}