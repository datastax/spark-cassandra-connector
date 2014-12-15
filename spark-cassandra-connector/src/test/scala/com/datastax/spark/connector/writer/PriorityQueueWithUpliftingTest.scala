package com.datastax.spark.connector.writer

import com.datastax.spark.connector.writer.GroupingBatchBuilder.PriorityQueueWithUplifting
import org.scalatest.{FlatSpec, Matchers}

class PriorityQueueWithUpliftingTest extends FlatSpec with Matchers {

  def emptyQueue = new PriorityQueueWithUplifting[Int](3)
  def fullQueue = {
    val q = new PriorityQueueWithUplifting[Int](1)
    q.add(7)
    q
  }

  "Empty PriorityQueueWithUplifting" should "have size 0" in {
    val pq = emptyQueue
    pq.size shouldBe 0
  }

  it should "throw NoSuchElementException when tried to remove an item" in {
    val pq = emptyQueue
    an [NoSuchElementException] shouldBe thrownBy { pq.remove() }
  }

  it should "throw IndexOutOfBoundsException when tried to access an item by index" in {
    val pq = emptyQueue
    an [IndexOutOfBoundsException] shouldBe thrownBy { pq.apply(0) }
  }

  it should "throw IndexOutOfBoundsException when tried to uplift an item" in {
    val pq = emptyQueue
    an [IndexOutOfBoundsException] shouldBe thrownBy { pq.update(0, 1) }
  }

  it should "allow to add an item and return its index" in {
    val pq = emptyQueue
    val box = pq.add(7)
    box._1 shouldBe 0
    box._2 shouldBe 7
    pq.size shouldBe 1
  }

  it should "allow to add and remove an item multiple times" in {
    val pq = emptyQueue
    pq.add(10)
    pq.remove()
    pq.add(20)
    pq.remove()
    pq.add(30)
    pq.remove()
    val box = pq.add(40)
    box._1 shouldBe 0
    box._2 shouldBe 40
    pq.size shouldBe 1
  }

  "Full PriorityQueueWithUplifting" should "have size equal to capacity" in {
    val pq = fullQueue
    pq.size shouldBe pq.capacity
  }

  it should "throw IllegalStateException when tried to add another item" in {
    val pq = fullQueue
    an [IllegalStateException] shouldBe thrownBy { pq.add(2) }
  }

  it should "allow to access an item by index" in {
    val pq = fullQueue
    pq.apply(0)._2 shouldBe 7
  }

  it should "allow to remove an item" in {
    val pq = fullQueue
    val item = pq.remove()
    item._2 shouldBe 7
    pq.size shouldBe 0
  }

  it should "allow to update an item" in {
    val pq = fullQueue
    val box = pq(0)
    pq.update(0, 8)
    pq(0) shouldBe theSameInstanceAs (box)
    pq(0)._2 shouldBe 8
  }

  it should "allow to remove and add an item multiple times while keeping items in order" in {
    val pq = new PriorityQueueWithUplifting[Int](3)
    pq.add(10)                // 10/0
    pq.add(20)                // 20/0, 10/1
    pq.add(30)                // 30/0, 20/1, 10/2
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((30, 20, 10))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((0, 1, 2))
    pq.remove()               // 20/1, 10/2
    pq.add(40)                // 40/1, 20/2, 10/0
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((40, 20, 10))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((1, 2, 0))
    pq.remove()               // 20/2, 10/0
    pq.add(15)                // 20/2, 15/0, 10/1
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((20, 15, 10))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((2, 0, 1))
    pq.remove()               // 15/0, 10/1
    pq.add(5)                 // 15/0, 10/1, 5/2
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((15, 10, 5))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((0, 1, 2))
    pq.size shouldBe 3
  }

  it should "maintain order while being updated" in {
    val pq = new PriorityQueueWithUplifting[Int](3)
    pq.add(10)                    // 10/0
    pq.add(20)                    // 20/0, 10/1
    pq.add(30)                    // 30/0, 20/1, 10/2
    pq.remove()
    pq.add(30)

    pq.update(1, 25)._2 shouldBe 25
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((30, 25, 10))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((1, 2, 0))

    pq.update(1, 35)._2 shouldBe 35
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((35, 30, 10))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((1, 2, 0))

    pq.update(0, 45)._2 shouldBe 45
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((45, 30, 10))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((1, 2, 0))

    pq.update(2, 55)._2 shouldBe 55
    (pq(0)._2, pq(1)._2, pq(2)._2) shouldBe ((55, 45, 30))
    (pq(0)._1, pq(1)._1, pq(2)._1) shouldBe ((1, 2, 0))
  }

}
