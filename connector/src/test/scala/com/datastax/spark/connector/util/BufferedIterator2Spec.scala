package com.datastax.spark.connector.util

import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable.ArrayBuffer

class BufferedIterator2Spec extends FlatSpec with Matchers {

  "BufferedIterator" should "return the same items as the standard Iterator" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 1
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 2
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 3
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 4
    iterator.hasNext shouldBe true
    iterator.next() shouldBe 5
    iterator.hasNext shouldBe false
  }

  it should "be convertible to a Seq" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    iterator.toSeq should contain inOrder(1, 2, 3, 4, 5)
  }

  it should "wrap an empty iterator" in {
    val iterator = new BufferedIterator2(Iterator.empty)
    iterator.isEmpty shouldBe true
    iterator.hasNext shouldBe false
  }

  it should "offer the head element without consuming the underlying iterator" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    iterator.head shouldBe 1
    iterator.next() shouldBe 1
  }

  it should "offer takeWhile that consumes only the elements matching the predicate" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    val firstThree = iterator.takeWhile(_ <= 3).toList

    firstThree should contain inOrder (1, 2, 3)
    iterator.head shouldBe 4
    iterator.next() shouldBe 4
  }

  it should "offer appendWhile that copies elements to ArrayBuffer and consumes only the elements matching the predicate" in {
    val iterator = new BufferedIterator2(Seq(1, 2, 3, 4, 5).iterator)
    val buffer = new ArrayBuffer[Int]
    iterator.appendWhile(_ <= 3, buffer)

    buffer should contain inOrder (1, 2, 3)
    iterator.head shouldBe 4
    iterator.next() shouldBe 4
  }

  it should "throw NoSuchElementException if trying to get next() element that doesn't exist" in {
    val iterator = new BufferedIterator2(Seq(1, 2).iterator)
    iterator.next()
    iterator.next()
    a [NoSuchElementException] should be thrownBy iterator.next()
  }
}
