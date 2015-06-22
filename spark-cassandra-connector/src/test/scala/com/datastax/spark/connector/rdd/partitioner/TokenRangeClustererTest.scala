package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.rdd.partitioner.dht.{CassandraNode, LongToken}
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class TokenRangeClustererTest {

  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[Long, LongToken]

  val addr1 = InetAddress.getByName("192.168.123.1")
  val addr2 = InetAddress.getByName("192.168.123.2")
  val addr3 = InetAddress.getByName("192.168.123.3")
  val addr4 = InetAddress.getByName("192.168.123.4")
  val addr5 = InetAddress.getByName("192.168.123.5")

  val node1 = CassandraNode(addr1, addr1)
  val node2 = CassandraNode(addr2, addr2)
  val node3 = CassandraNode(addr3, addr3)
  val node4 = CassandraNode(addr4, addr4)
  val node5 = CassandraNode(addr5, addr5)

  private def token(x: Long) = new com.datastax.spark.connector.rdd.partitioner.dht.LongToken(x)

  @Test
  def testEmpty() {
    val trc = new TokenRangeClusterer(10)
    val groups = trc.group(Seq.empty)
    assertEquals(0, groups.size)
  }

  @Test
  def testTrivialClustering() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), Some(5))
    val tr2 = new TokenRange(token(10), token(20), Set(node1), Some(5))
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2))
    assertEquals(1, groups.size)
    assertEquals(Set(tr1, tr2), groups.head.ranges.toSet)
  }

  @Test
  def testSplitByHost() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), Some(2))
    val tr2 = new TokenRange(token(10), token(20), Set(node1), Some(2))
    val tr3 = new TokenRange(token(20), token(30), Set(node2), Some(2))
    val tr4 = new TokenRange(token(30), token(40), Set(node2), Some(2))

    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.ranges.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testSplitByCount() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), Some(5))
    val tr2 = new TokenRange(token(10), token(20), Set(node1), Some(5))
    val tr3 = new TokenRange(token(20), token(30), Set(node1), Some(5))
    val tr4 = new TokenRange(token(30), token(40), Set(node1), Some(5))

    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4)).map(_.ranges.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1, tr2)))
    assertTrue(groups.contains(Set(tr3, tr4)))
  }

  @Test
  def testTooLargeRanges() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1), Some(100000))
    val tr2 = new TokenRange(token(10), token(20), Set(node1), Some(100000))
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2)).map(_.ranges.toSet).toSet
    assertEquals(2, groups.size)
    assertTrue(groups.contains(Set(tr1)))
    assertTrue(groups.contains(Set(tr2)))
  }

  @Test
  def testMultipleEndpoints() {
    val tr1 = new TokenRange(token(0), token(10), Set(node2, node1, node3), Some(1))
    val tr2 = new TokenRange(token(10), token(20), Set(node1, node3, node4), Some(1))
    val tr3 = new TokenRange(token(20), token(30), Set(node3, node1, node5), Some(1))
    val tr4 = new TokenRange(token(30), token(40), Set(node3, node1, node4), Some(1))
    val trc = new TokenRangeClusterer[Long, LongToken](10)
    val groups = trc.group(Seq(tr1, tr2, tr3, tr4))
    assertEquals(1, groups.size)
    assertEquals(4, groups.head.ranges.size)
    assertFalse(groups.head.endpoints.isEmpty)
  }

  @Test
  def testMaxClusterSize() {
    val tr1 = new TokenRange(token(0), token(10), Set(node1, node2, node3), Some(1))
    val tr2 = new TokenRange(token(10), token(20), Set(node1, node2, node3), Some(1))
    val tr3 = new TokenRange(token(20), token(30), Set(node1, node2, node3), Some(1))
    val trc = new TokenRangeClusterer[Long, LongToken](maxRowCountPerGroup = 10, maxGroupSize = 1)
    val groups = trc.group(Seq(tr1, tr2, tr3))
    assertEquals(3, groups.size)
  }


  @Test
  def testRealClusterSize() {
    val rnd = new Random(1);
    val nodes= (1 to 8).map(i=>InetAddress.getByName("192.168.123." + i)).map (a=>CassandraNode(a, a))
    val trs = (1 to 256 * 8).map(i=> new TokenRange(token(i*10), token((i+1)*10), rnd.shuffle(nodes).take(3).toSet, Some(1)))
    val trc = new TokenRangeClusterer[Long, LongToken](maxRowCountPerGroup = 256)
    val groups = trc.group(trs)
    groups.map(g => (g.endpoints, g.rowCount)).groupBy(_._1).mapValues(_.map(_._2).sum).foreach(println)
    assertEquals(14, groups.size)
  }
}
