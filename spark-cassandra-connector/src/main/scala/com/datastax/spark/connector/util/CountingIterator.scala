package com.datastax.spark.connector.util

/** Counts elements fetched form the underlying iterator. */
class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
  private var _count = 0

  /** Returns the number of successful invocations of `next` */
  def count = _count

  def hasNext = iterator.hasNext

  def next() = {
    val item = iterator.next()
    _count += 1
    item
  }
}
