package com.datastax.spark.connector.util

/** Counts elements fetched form the underlying iterator. Limit causes iterator to terminate early */
class CountingIterator[T](iterator: Iterator[T], limit: Option[Long] = None) extends Iterator[T] {
  private var _count = 0

  /** Returns the number of successful invocations of `next` */
  def count = _count

  def hasNext = limit match {
    case Some(l) => _count < l && iterator.hasNext
    case _ => iterator.hasNext
  }

  def next() = {
    val item = iterator.next()
    _count += 1
    item
  }
}
