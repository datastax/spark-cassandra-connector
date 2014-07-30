package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.rdd.partitioner.dht.LongToken
import org.junit.Assert._
import org.junit.Test

class TokenRangeClustererTest {

  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[Long, LongToken]

  val addr1 = InetAddress.getByName("192.168.123.1")
  val addr2 = InetAddress.getByName("192.168.123.2")
  val addr3 = InetAddress.getByName("192.168.123.3")
  val addr4 = InetAddress.getByName("192.168.123.4")
  val addr5 = InetAddress.getByName("192.168.123.5")

  private def token(x: Long) = new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(x)

  @Test
  def testEmpty() {
    val trc = new TokenRangeClusterer(10)
    val groups = trc.group(Seq.empty)
    assertEquals(0, groups.size)
  }

  @Test
  def testTrivialClustering() {
    val tr1 = new TokenRange(token(0), token(10), Set(addr1), Some(5))
    val tr2 = new TokenRange(token(10), token(20), Set(addr1), Some(5))
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2))
    assertEquals(1, groups.size)
    assertEquals(Set(tr1, tr2), groups.head.toSet)
  }

  @Test
  def testSplitByHost() {
    val tr1 = new TokenRange(token(0), token(10), Set(addr1), Some(2))
    val tr2 = new TokenRange(token(10), token(20), Set(addr1), Some(2))
    val tr3 = new TokenRange(token(20), token(30), Set(addr2), Some(2))
    val tr4 = new TokenRange(token(30), token(40), Set(addr2), Some(2))

    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testSplitByCount() {
    val tr1 = new TokenRange(token(0), token(10), Set(addr1), Some(5))
    val tr2 = new TokenRange(token(10), token(20), Set(addr1), Some(5))
    val tr3 = new TokenRange(token(20), token(30), Set(addr1), Some(5))
    val tr4 = new TokenRange(token(30), token(40), Set(addr1), Some(5))

    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testTooLargeRanges() {
    val tr1 = new TokenRange(token(0), token(10), Set(addr1), Some(100000))
    val tr2 = new TokenRange(token(10), token(20), Set(addr1), Some(100000))
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2)).map(_.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1)))
    assertTrue(groups.contains(Set(tr2)))
  }

  @Test
  def testMultipleEndpoints() {
    val tr1 = new TokenRange(token(0), token(10), Set(addr2, addr1, addr3), Some(1))
    val tr2 = new TokenRange(token(10), token(20), Set(addr1, addr3, addr4), Some(1))
    val tr3 = new TokenRange(token(20), token(30), Set(addr3, addr1, addr5), Some(1))
    val tr4 = new TokenRange(token(30), token(40), Set(addr3, addr1, addr4), Some(1))
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4))
    assertEquals(1, groups.size)
    assertEquals(4, groups.head.size)
    assertFalse(groups.head.map(_.endpoints).reduce(_ intersect _).isEmpty)
  }

  @Test
  def testMaxClusterSize() {
    val tr1 = new TokenRange(token(0), token(10), Set(addr1, addr2, addr3), Some(1))
    val tr2 = new TokenRange(token(10), token(20), Set(addr1, addr2, addr3), Some(1))
    val tr3 = new TokenRange(token(20), token(30), Set(addr1, addr2, addr3), Some(1))
    val trc = new TokenRangeClusterer[Long, LongToken](maxRowCountPerGroup = 10, maxGroupSize = 1)
    val groups = trc.group(Seq(tr1, tr2, tr3))
    assertEquals(3, groups.size)
  }

}
