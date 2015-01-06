package com.datastax.spark.connector.writer

import java.util

import com.datastax.driver.core._
import com.datastax.spark.connector.BatchSize
import com.datastax.spark.connector.util.PriorityHashMap
import com.google.common.collect.AbstractIterator
import org.apache.spark.util.MutablePair

import scala.annotation.tailrec
import scala.collection.Iterator
import scala.collection.mutable
import scala.reflect.ClassTag

class GroupingBatchBuilder[T](batchStatementBuilder: BatchStatementBuilder[T],
                              batchKeyGenerator: BoundStatement => Any,
                              batchSize: BatchSize,
                              maxBatches: Int,
                              data: Iterator[T]) extends AbstractIterator[Statement] with Iterator[Statement] {
  require(maxBatches > 0)

  private[this] val batchMap = new PriorityHashMap[Any, Batch](maxBatches)
  private[this] val emptyBatches = new util.Stack[Batch]()
  emptyBatches.ensureCapacity(maxBatches + 1)

  private[this] var lastStatement: BoundStatement = null

  private def processStatement(boundStatement: BoundStatement): Option[Batch] = {
    val batchKey = batchKeyGenerator(boundStatement)
    batchMap.get(batchKey) match {
      case Some(batch) =>
        updateBatchInMap(batchKey, batch, boundStatement)
      case None =>
        addBatchToMap(batchKey, boundStatement)
    }
  }

  private def updateBatchInMap(batchKey: Any, batch: Batch, newStatement: BoundStatement): Option[Batch] = {
    if (batch.add(newStatement, force = false)) {
      batchMap.put(batchKey, batch)
      None
    } else {
      batchMap.remove(batchKey)
      Some(batch)
    }
  }

  private def addBatchToMap(batchKey: Any, newStatement: BoundStatement): Option[Batch] = {
    if (batchMap.size == maxBatches) {
      Some(batchMap.dequeue())
    } else {
      val batch = newBatch()
      batch.add(newStatement, force = true)
      batchMap.put(batchKey, batch)
      None
    }
  }

  private def newBatch(): Batch = {
    if (emptyBatches.isEmpty)
      Batch(batchSize)
    else
      emptyBatches.pop()
  }

  private def createStmtAndReleaseBatch(batch: Batch): Statement = {
    val stmt = batchStatementBuilder.maybeCreateBatch(batch.statements)
    batch.clear()
    emptyBatches.push(batch)
    stmt
  }

  @tailrec
  final override def computeNext(): Statement = {
    if (lastStatement == null && data.hasNext) {
      lastStatement = batchStatementBuilder.bind(data.next())
    }

    if (lastStatement != null) {
      processStatement(lastStatement) match {
        case Some(batch) =>
          createStmtAndReleaseBatch(batch)
        case None =>
          lastStatement = null
          computeNext()
      }
    } else {
      if (batchMap.nonEmpty)
        createStmtAndReleaseBatch(batchMap.dequeue())
      else
        endOfData()
    }
  }

}

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


  private[connector] class QueuedHashMap[K, V: Ordering : ClassTag](capacity: Int) {

    type Box = MutablePair[Int, (K, V)]

    implicit val ordering = new Ordering[(K, V)] {
      override def compare(x: (K, V), y: (K, V)): Int = implicitly[Ordering[V]].compare(x._2, y._2)
    }

    private[this] val data = new PriorityQueueWithUplifting[(K, V)](capacity)
    private[this] val keyMap = mutable.HashMap[K, Box]().withDefaultValue(null)

    /** Retrieves the element by key */
    def apply(key: K): Option[V] = keyMap(key) match {
      case null =>
        None
      case box: Box =>
        Some(box._2._2)
    }

    /** Assumes that the box at the given key has been updated. It performs required updates in
      * the data structures. */
    def update(key: K): Unit = data.update(keyMap(key))

    /** Adds a new element to the map. */
    def add(key: K, value: V): Unit = {
      if (keyMap.contains(key))
        throw new IllegalStateException(s"Key $key already exists")

      val box = data.add((key, value))
      keyMap.put(key, box)
    }

    /** Removes and returns the top element. */
    def remove(): V = {
      val box = data.remove()
      keyMap.remove(box._2._1)
      box._2._2
    }

    /** Returns the size of the map. */
    def size(): Int = data.size

    /** Returns the top element of the map. */
    def head(): V = data.apply(0)._2._2

  }

}
