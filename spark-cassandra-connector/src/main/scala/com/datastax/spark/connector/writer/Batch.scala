package com.datastax.spark.connector.writer

import com.datastax.driver.core.BoundStatement
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}

import scala.collection.mutable.ArrayBuffer

/** A simple wrapper over a collection of bound statements. */
private[writer] sealed trait Batch extends Ordered[Batch] {
  /** Returns `true` if the element has been successfully added. Returns `false` if the element
    * cannot be added because adding it would violate the size limitation. If `force` is set to `true`,
    * it adds the item regardless of size limitations and always returns `true`. */
  def add(stmt: BoundStatement, force: Boolean = false): Boolean

  /** Collected statements */
  def statements: Seq[BoundStatement]

  /** Only for internal use - batches are compared by this value. */
  protected[Batch] def size: Int

  override def compare(that: Batch): Int = size.compareTo(that.size)

  /** Removes all the collected statements and resets this batch to the initial state. */
  def clear(): Unit
}

private[writer] object Batch {

  implicit val batchOrdering = Ordering.ordered[Batch]

  def apply(batchSize: BatchSize): Batch = {
    batchSize match {
      case RowsInBatch(rows) => new RowLimitedBatch(rows)
      case BytesInBatch(bytes) => new SizeLimitedBatch(bytes)
    }
  }
}

/** The implementation which uses the number of items as a size constraint. */
private[writer] class RowLimitedBatch(val maxRows: Int) extends Batch {
  private val buf = new ArrayBuffer[BoundStatement](maxRows)

  override def add(stmt: BoundStatement, force: Boolean = false): Boolean = {
    if (!force && buf.size >= maxRows) {
      false
    } else {
      buf += stmt
      true
    }
  }

  override def size = buf.size

  override def statements: Seq[BoundStatement] = buf

  override def clear(): Unit = buf.clear()
}

/** The implementation which uses length in bytes as a size constraint. */
private[writer] class SizeLimitedBatch(val maxBytes: Int) extends Batch {
  private val buf = new ArrayBuffer[BoundStatement](10)
  private var _size = 0

  override def add(stmt: BoundStatement, force: Boolean = false): Boolean = {
    val stmtSize = BatchStatementBuilder.calculateDataSize(stmt)
    // buf.nonEmpty here is to allow adding at least a single statement regardless its size
    if (!force && (_size + stmtSize) > maxBytes) {
      false
    } else {
      buf += stmt
      _size += stmtSize
      true
    }
  }

  override def size = _size

  override def statements: Seq[BoundStatement] = buf

  override def clear(): Unit = {
    _size = 0
    buf.clear()
  }
}
