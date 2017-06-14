package com.datastax.spark.connector.util

import scala.collection.mutable.ArrayBuffer

/** An iterator that groups items having the same value of the given function (key).
  * To be included in the same group, items with the same key must be next to each other
  * in the original collection.
  *
  * `SpanningIterator` buffers internally one group at a time and the wrapped iterator
  * is consumed in a lazy way.
  *
  * Example:
  * {{{
  *   val collection = Seq(1 -> "a", 1 -> "b", 1 -> "c", 2 -> "d", 2 -> "e")
  *   val iterator = new SpanningIterator(collection.iterator, (x: (Int, String)) => x._1)
  *   val result = iterator.toSeq  // Seq(1 -> Seq("a", "b", "c"), 2 -> Seq("d", "e"))
  * }}}
  */
class SpanningIterator[K, T](iterator: Iterator[T], f: T => K) extends Iterator[(K, Seq[T])] {

  private[this] val items = new BufferedIterator2(iterator)

  override def hasNext = items.hasNext

  override def next(): (K, Seq[T]) = {
    val key = f(items.head)
    val buffer = new ArrayBuffer[T]
    items.appendWhile(r => f(r) == key, buffer)
    (key, buffer)
  }
}
