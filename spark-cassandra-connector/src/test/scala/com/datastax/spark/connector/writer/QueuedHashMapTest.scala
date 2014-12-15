package com.datastax.spark.connector.writer

import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.writer.GroupingBatchBuilder.QueuedHashMap
import org.scalatest.{FlatSpec, Matchers}

class QueuedHashMapTest extends FlatSpec with Matchers {
  implicit val atomicIntegerOrdering = new Ordering[AtomicInteger] {
    override def compare(x: AtomicInteger, y: AtomicInteger): Int = x.intValue() compare y.intValue()
  }

  "QueuedHashMap" should "allow to add elements" in {
    val qmap = new QueuedHashMap[String, Int](3)
    qmap.add("a", 10)
    qmap.add("b", 20)

    qmap.size() shouldBe 2
    qmap.head() shouldBe 20
    qmap("a") shouldBe Some(10)
    qmap("b") shouldBe Some(20)
  }

  it should "allow to update elements" in {
    val qmap = new QueuedHashMap[String, AtomicInteger](3)
    val a = new AtomicInteger(10)
    val b = new AtomicInteger(20)
    val c = new AtomicInteger(30)
    qmap.add("a", a)
    qmap.add("b", b)
    qmap.add("c", c)

    qmap.head() shouldBe theSameInstanceAs (c)
    qmap("b") shouldBe Some(b)
    qmap("b").get.set(40)
    qmap.update("b")
    qmap.head() shouldBe theSameInstanceAs (b)

    qmap.size() shouldBe 3
  }

  it should "allow to remove elements" in {
    val qmap = new QueuedHashMap[String, Int](3)
    qmap.add("a", 10)
    qmap.add("b", 20)
    qmap.add("c", 30)

    qmap.size() shouldBe 3
    qmap.remove() shouldBe 30
    qmap.remove() shouldBe 20
    qmap.remove() shouldBe 10
    qmap.size() shouldBe 0
  }

}
