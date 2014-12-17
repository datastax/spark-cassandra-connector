package com.datastax.spark.connector.writer

import com.datastax.driver.core.BoundStatement
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}

import scala.collection.mutable.ArrayBuffer

/** A simple wrapper over a collection of bound statements. */
private[writer] sealed trait Batch extends Ordered[Batch] {
  def add(stmt: BoundStatement): Unit

  /** Returns `true` if the collected statements exceed the desired size of this batch. Rows based and
    * bytes based implementations compute it in different ways. */
  def isSizeExceeded: Boolean

  def statements: Seq[BoundStatement]

  /** Removes all the collected statements and resets this batch to the initial state. */
  def reuse(): Unit
}

private[writer] object Batch {
  implicit val batchOrdering = new Ordering[Batch] {
    override def compare(x: Batch, y: Batch): Int = x.compare(y)
  }

  def apply(batchSize: BatchSize): Batch = {
    batchSize match {
      case RowsInBatch(rows) => new RowLimitedBatch(rows)
      case BytesInBatch(bytes) => new SizeLimitedBatch(bytes)
    }
  }
}

private[writer] class RowLimitedBatch(val maxRows: Int) extends Batch {
  val buf = new ArrayBuffer[BoundStatement](maxRows)

  def add(stmt: BoundStatement): Unit = {
    buf += stmt
  }

  override def isSizeExceeded: Boolean = buf.size > maxRows

  override def compare(that: Batch): Int = that match {
    case thatBatch: RowLimitedBatch => buf.size.compare(thatBatch.maxRows)
    case _ => throw new ClassCastException("Not a RowLimitedBatch")
  }

  override def statements: Seq[BoundStatement] = buf

  override def reuse(): Unit = buf.clear()
}

private[writer] class SizeLimitedBatch(val maxBytes: Int) extends Batch {
  val buf = new ArrayBuffer[BoundStatement](10)
  var size = 0

  def add(stmt: BoundStatement): Unit = {
    buf += stmt
    size += BatchStatementBuilder.calculateDataSize(stmt)
  }

  override def isSizeExceeded: Boolean = size > maxBytes

  override def compare(that: Batch): Int = that match {
    case thatBatch: SizeLimitedBatch => size.compare(thatBatch.size)
    case _ => throw new ClassCastException("Not a SizeLimitedBatch")
  }

  override def statements: Seq[BoundStatement] = buf

  override def reuse(): Unit = {
    size = 0
    buf.clear()
  }
}
