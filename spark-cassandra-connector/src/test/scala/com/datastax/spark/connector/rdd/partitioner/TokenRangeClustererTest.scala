package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.junit.Assert._
import org.junit.Test

import com.datastax.spark.connector.rdd.partitioner.dht.LongToken

class TokenRangeClustererTest {

  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[Long, LongToken]

  val node1 = InetAddress.getByName("192.168.123.1")
  val node2 = InetAddress.getByName("192.168.123.2")
  val node3 = InetAddress.getByName("192.168.123.3")
  val node4 = InetAddress.getByName("192.168.123.4")
  val node5 = InetAddress.getByName("192.168.123.5")

  private def token(x: Long) = new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(x)

  @Test
  def testEmpty() {
    val trc = new TokenRangeClusterer(10)
    val groups = trc.group(Seq.empty)
    assertEquals(0, groups.size)
  }

  @Test
  def testTrivialClustering() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), 5)
    val tr2 = new TokenRange(token(10), token(20), Set(node1), 5)
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2))
    assertEquals(1, groups.size)
    assertEquals(Set(tr1, tr2), groups.head.toSet)
  }

  @Test
  def testSplitByHost() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), 2)
    val tr2 = new TokenRange(token(10), token(20), Set(node1), 2)
    val tr3 = new TokenRange(token(20), token(30), Set(node2), 2)
    val tr4 = new TokenRange(token(30), token(40), Set(node2), 2)

    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testSplitByCount() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), 5)
    val tr2 = new TokenRange(token(10), token(20), Set(node1), 5)
    val tr3 = new TokenRange(token(20), token(30), Set(node1), 5)
    val tr4 = new TokenRange(token(30), token(40), Set(node1), 5)

    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testTooLargeRanges() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), 100000)
    val tr2 = new TokenRange(token(10), token(20), Set(node1), 100000)
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1)))
    assertTrue(groups.contains(Set(tr2)))
  }

  @Test
  def testMultipleEndpoints() {
    val tr1 = new TokenRange(token(0), token(10), Set(node2, node1, node3), 1)
    val tr2 = new TokenRange(token(10), token(20), Set(node1, node3, node4), 1)
    val tr3 = new TokenRange(token(20), token(30), Set(node3, node1, node5), 1)
    val tr4 = new TokenRange(token(30), token(40), Set(node3, node1, node4), 1)
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4))
    assertEquals(1, groups.size)
    assertEquals(4, groups.head.size)
    assertFalse(groups.head.map(_.replicas).reduce(_ intersect _).isEmpty)
  }

  @Test
  def testMaxClusterSize() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1, node2, node3), 1)
    val tr2 = new TokenRange(token(10), token(20), Set(node1, node2, node3), 1)
    val tr3 = new TokenRange(token(20), token(30), Set(node1, node2, node3), 1)
    val trc = new TokenRangeClusterer[Long, LongToken](maxRowCountPerGroup = 10, maxGroupSize = 1)
    val groups = trc.group(Seq(tr1, tr2, tr3))
    assertEquals(3, groups.size)
  }

}
