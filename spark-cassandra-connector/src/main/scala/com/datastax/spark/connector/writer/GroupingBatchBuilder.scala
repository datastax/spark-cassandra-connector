package com.datastax.spark.connector.writer

import java.util

import com.datastax.driver.core._
import com.datastax.spark.connector.BatchSize
import com.datastax.spark.connector.util.PriorityHashMap
import com.google.common.collect.AbstractIterator

import scala.annotation.tailrec
import scala.collection.Iterator

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
