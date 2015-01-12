package com.datastax.spark.connector.util

import scala.collection.mutable.ArrayBuffer

/** Serves the same purpose as `BufferedIterator` in Scala, but its `takeWhile` method 
  * properly doesn't consume the next element. */
class BufferedIterator2[T](iterator: Iterator[T]) extends Iterator[T] {

  // Instead of a pair of T and Boolean we could use Option, but
  // this would allocate a new Option object per each item, which might be
  // too big overhead in tight loops using this iterator.
  private[this] var headDefined: Boolean = false
  private[this] var headElement: T = advance()

  def head =
    if (headDefined) headElement
    else throw new NoSuchElementException("Head of empty iterator")

  def headOption = headElement

  private def advance(): T = {
    if (iterator.hasNext) {
      headDefined = true
      iterator.next()
    }
    else {
      headDefined = false
      null.asInstanceOf[T]
    }
  }

  override def hasNext = headDefined

  override def next() = {
    val result = head
    headElement = advance()
    result
  }


  override def takeWhile(p: T => Boolean): Iterator[T] = {
    new Iterator[T]() {
      override def hasNext = headDefined && p(headElement)
      override def next() =
        if (hasNext) BufferedIterator2.this.next()
        else throw new NoSuchElementException
    }
  }

  def appendWhile(p: (T) => Boolean, target: ArrayBuffer[T]): Unit = {
    while (headDefined && p(headElement)) {
      target += headElement
      headElement = advance()
    }
  }
}
