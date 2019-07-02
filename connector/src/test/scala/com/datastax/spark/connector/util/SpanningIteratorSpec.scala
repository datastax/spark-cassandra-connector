package com.datastax.spark.connector.util

import org.scalatest.{FlatSpec, Matchers}

class SpanningIteratorSpec extends FlatSpec with Matchers {

  "SpanningIterator" should "group an empty collection" in {
    new SpanningIterator[Int, Int](Iterator.empty, identity).isEmpty shouldBe true
  }

  it should "group a sequence of elements with the same key into a single item and should preserve order" in {
    val collection = Seq(1, 2, 3, 4, 5)
    val grouped = new SpanningIterator(collection.iterator, (_: Int) => 0).toSeq
    grouped should have length 1
    grouped.head._2 should contain inOrder(1, 2, 3, 4, 5)
  }

  it should "group a sequence of elements with distinct keys the same number of groups" in {
    val collection = Seq(1, 2, 3, 4, 5)
    val grouped = new SpanningIterator(collection.iterator, identity[Int]).toSeq
    grouped should have length 5
    grouped.distinct should have length 5 // to check if something wasn't included more than once
  }

  it should "group a sequence of elements with two keys into two groups" in {
    val collection = Seq(1 -> 10, 1 -> 11, 1 -> 12, 2 -> 20, 2 -> 21)
    val grouped = new SpanningIterator(collection.iterator, (x: (Int, Int)) => x._1).toIndexedSeq
    grouped should have length 2
    grouped(0)._1 should be(1)
    grouped(0)._2 should contain inOrder(1 -> 10, 1 -> 11, 1 -> 12)
    grouped(1)._1 should be(2)
    grouped(1)._2 should contain inOrder(2 -> 20, 2 -> 21)
  }

  it should "be lazy and work with infinite streams" in {
    val stream = Stream.from(0)
    val grouped = new SpanningIterator(stream.iterator, identity[Int])
    grouped.take(5).toSeq.map(_._1) should contain inOrder(0, 1, 2, 3, 4)
  }
}
