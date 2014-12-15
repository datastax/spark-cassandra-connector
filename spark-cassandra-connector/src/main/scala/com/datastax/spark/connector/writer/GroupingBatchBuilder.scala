package com.datastax.spark.connector.writer

import org.apache.spark.util.MutablePair

import scala.annotation.tailrec
import scala.reflect.ClassTag

object GroupingBatchBuilder {

  /**
   * This class has the similar functionality as the heap based priority queue. However, it is optimized
   * for the particular operations. The queue is sorted in descending order according to the provided
   * ordering.
   *
   * A new item is added in the end of the queue and then moved towards the head as long as it is greater
   * than its immediate predecessor. Removal of the first element is O(1) because we just need to move
   * the head pointer. Accessing element by index is O(1).
   *
   * It offers updating an element. This operation involves moving the element towards the head of the
   * queue so that the order is preserved. However, this is limited to only to moving elements forward,
   * that is, the element can be update only the way that it is not lower than it was before.
   */
  class PriorityQueueWithUplifting[V: Ordering : ClassTag](val capacity: Int) {
    /** A mutable pair which contains a real index and a value */
    type Box = MutablePair[Int, V]

    private[this] val buf = new Array[Box](capacity)
    private[this] var startPos = 0
    private[this] var curSize = 0
    private[this] val ordering = implicitly[Ordering[V]]

    def size: Int = curSize

    /** Returns an item at the given position. */
    def apply(idx: Int): Box = {
      if (idx < 0 || idx >= curSize)
        throw new IndexOutOfBoundsException()

      buf(normalize(idx + startPos))
    }

    /** Updates a box at the given position with the provided value. Then, it moves the box towards the
      * head of the queue if needed. Box indexes are updated accordingly. */
    def update(idx: Int, value: V): Box = {
      if (idx < 0 || idx >= curSize)
        throw new IndexOutOfBoundsException()

      val box = setUnchecked(normalize(idx + startPos), value)
      upliftIfNeeded(box._1)
    }

    /** Updates a box with the provided value. Then, it moves the box towards the
      * head of the queue if needed. Box indexes are updated accordingly. */
    def update(box: Box): Box = {
      val b = setUnchecked(normalize(box._1), box._2)
      upliftIfNeeded(b._1)
    }

    /** Removes and returns the first element of the queue. */
    def remove(): Box = {
      if (curSize > 0) {
        val result = buf(startPos)
        startPos = normalize(startPos + 1)
        curSize -= 1
        result
      } else {
        throw new NoSuchElementException("The queue is empty")
      }
    }

    /** Adds a value the end of the queue and then moves it toward the head if needed. */
    def add(value: V): Box = {
      if (curSize >= capacity)
        throw new IllegalStateException("The queue is full")

      curSize += 1
      update(curSize - 1, value)
    }

    @inline
    private[this] def normalize(x: Int): Int =
      if (x < 0)
        capacity + (x % capacity)
      else
        x % capacity

    @tailrec
    private[this] def upliftIfNeeded(realIdx: Int): Box = {
      normalize(realIdx) match {
        case pos if pos == startPos => buf(startPos)
        case pos =>
          val prev = normalize(pos - 1)
          if (ordering.compare(buf(pos)._2, buf(prev)._2) > 0) {
            swapUnchecked(pos, prev)
            upliftIfNeeded(prev)
          } else {
            buf(pos)
          }
      }
    }

    private[this] def swapUnchecked(realIdx1: Int, realIdx2: Int): Unit = {
      val box1 = buf(realIdx1)
      buf(realIdx1) = buf(realIdx2)
      buf(realIdx2) = box1
      buf(realIdx1)._1 = realIdx1
      buf(realIdx2)._1 = realIdx2
    }


    private[this] def setUnchecked(realIdx: Int, value: V): Box = {
      if (buf(realIdx) == null) {
        buf(realIdx) = new Box(realIdx, value)
        buf(realIdx)
      } else
        buf(realIdx).update(realIdx, value)
    }
  }

}
