package com.datastax.spark.connector.util

import scala.annotation.tailrec

/**
 * A HashMap and a PriorityQueue hybrid.
 * Works like a HashMap but offers additional O(1) access to the entry with
 * the highest value. As in a standard HashMap, entries can be looked up by
 * key in O(1) time. Adding, removing and updating items by key is handled
 * in O(log n) time.
 *
 * Keys must not be changed externally and must implement
 * proper equals and hashCode. It is advised to use immutable classes for keys.
 *
 * Values must be properly comparable.
 * Values may be externally mutated as long as a proper immediate call to `put`
 * is issued to notify the PriorityHashMap that the value associated with the given key
 * has changed, after each value mutation.
 * It is not allowed to externally mutate more than one value
 * at a time or to mutate a value associated with multiple keys.
 * Therefore, it is advised to use immutable classes for values, and updating
 * values only by calls to `put`.
 *
 * Contrary to standard Java HashMap implementation, PriorityHashMap does not
 * allocate memory on adding / removing / updating items and stores
 * all data in flat, non-resizable arrays instead. Therefore its
 * capacity cannot be modified after construction. It is technically possible
 * to remove this limitation in the future.
 *
 * PriorityHashMap is not thread-safe.
 *
 * Internally, PriorityHashMap is composed of three data arrays:
 * - an array storing references to keys, forming a heap-based priority queue;
 * - an array storing corresponding references to values, always in the same order as keys;
 * - an array storing indexes into the first two arrays, used as an inline hash-table allowing to
 *   quickly locate keys in the heap in constant time.
 *
 * The indexes hash-table doesn't use overflow lists for dealing with hash collisions.
 * The overflow entries are placed in the main hash-table array in the first not-taken
 * entry to the right from the original position pointed by key hash. On search,
 * if the key is not found immediately at a position pointed by key hash, it is searched
 * to the right, until it is found or an empty array entry is found.
 *
 * @param _capacity minimum required capacity of this collection; the actual capacity may be larger than this,
 *                  because for performance reasons it is rounded up to the nearest power of two
 * @tparam K type of keys
 * @tparam V type of values; values must be comparable
 */
class PriorityHashMap[K, V : Ordering](_capacity: Int) {

  private[this] var _size = 0
  def size = _size
  def isEmpty = _size == 0
  def nonEmpty = !isEmpty

  private def log2(n: Int): Int = 32 - Integer.numberOfLeadingZeros(n - 1)
  private def pow2(n: Int): Int = math.pow(2, n).toInt

  /** The maximum number of items that can be stored at a time in this map. */
  val capacity = pow2(log2(math.max(_capacity, 2)))

  // indexes array is twice bigger than capacity,
  // so we need to use twice bigger hashing mask as well;
  // the mask allows to filter appropriate number of less significant bits of the hash
  private val mask = (capacity * 2) - 1

  /** Original hash multiplied by 2, to hash into even entries only, so there is
    * initially at least one empty entry between them (works as a search-terminator).
    * This is to protect against bad hashes forming long sequences
    * of consecutive numbers, which would result in O(n) lookup, instead of O(1), even
    * if there were no hash-collisions. */
  private def hash(key: K): Int =
    (key.hashCode() << 1) & mask

  private[this] val _keys = Array.ofDim[AnyRef](capacity).asInstanceOf[Array[K]]
  private[this] val _values = Array.ofDim[AnyRef](capacity).asInstanceOf[Array[V]]
  private[this] val _indexes = Array.fill(capacity * 2)(-1)  // hash-table twice as big, to account for hash-collisions
  private[this] val ordering = implicitly[Ordering[V]]

  /** Finds a key in the indexes array.
    * Returns a position in the indexes array pointing to the found key or a position of
    * the first empty index entry, if key was not found. */
  private def find(key: K): Int = {
    find(key, hash(key))
  }

  /** Finds a key in the indexes array, starting at a given position in the indexes array
    * Returns a position in the indexes array pointing to the found key or a position of
    * the first empty index entry, if key was not found. */
  @tailrec
  private def find(key: K, pos: Int): Int = {
    val i = _indexes(pos)
    if (i < 0 || _keys(i) == key) pos
    else find(key, (pos + 1) & mask)
  }

  /** Records a new position of the key in the index array. */
  private def setIndex(key: K, index: Int): Unit =
    _indexes(find(key)) = index

  /** Fixes the position of the key in the indexes array.
    * Required after removal of keys from the indexes array. */
  @tailrec
  private def rehash(pos: Int): Unit = {
    val index = _indexes(pos)
    if (index >= 0) {
      val key = _keys(index)
      _indexes(pos) = -1
      setIndex(key, index)
      rehash((pos + 1) & mask)
    }
  }

  /** Removes an entry from the hash table */
  private def removeIndex(pos: Int): Unit = {
    _indexes(pos) = -1
    rehash((pos + 1) & mask)
  }

  /** Swaps two items in the given array */
  private def swap[T](array: Array[T], i1: Int, i2: Int): Unit = {
    val tmp = array(i1)
    array(i1) = array(i2)
    array(i2) = tmp
  }

  /** Swaps two keys/values in the heap and updates their indexes in the indexes hash-table */
  private def swap(i1: Int, i2: Int): Unit = {
    val k1 = _keys(i1)
    val k2 = _keys(i2)
    // indexes must be updated first, because setIndex needs correct keys() array to find the key.
    // if we swapped keys earlier, the key could not be found
    setIndex(k1, i2)
    setIndex(k2, i1)
    swap(_keys, i1, i2)
    swap(_values, i1, i2)
  }

  /** Moves a key/value to a new position in the heap and updates the indexes hash-table appropriately */
  private def move(from: Int, to: Int): Unit = {
    _keys(to) = _keys(from)
    _values(to) = _values(from)
    setIndex(_keys(to), to)
  }

  /** Clears given key/value pair of the heap i.e. sets them to null.
    * This is to make sure we don't keep any references to the removed items so GC could clean them up. */
  private def clear(index: Int): Unit = {
    _keys.asInstanceOf[Array[AnyRef]](index) = null
    _values.asInstanceOf[Array[AnyRef]](index) = null
  }

  /** Returns the index of the left child of the given entry in the heap */
  private def left(index: Int) = (index << 1) + 1
  /** Returns the index of the right child of the given entry in the heap */
  private def right(index: Int) = (index << 1) + 2
  /** Returns the index of the parent of the given entry in the heap */
  private def parent(index: Int) = (index - 1) >>> 1

  private def isValidIndex(index: Int) = index < _size
  private def hasLeft(index: Int) = isValidIndex(left(index))
  private def hasRight(index: Int) = isValidIndex(right(index))
  private def hasParent(index: Int) = index > 0

  /** Returns the index of the child on the heap that has the highest value */
  private def indexOfMaxChild(index: Int): Int = {
    val leftIndex = left(index)
    val leftValue = _values(leftIndex)
    if (hasRight(index)) {
      val rightIndex = right(index)
      val rightValue = _values(rightIndex)
      if (ordering.compare(leftValue, rightValue) > 0) leftIndex else rightIndex
    }
    else {
      leftIndex
    }
  }

  /** Maintains the heap invariant by moving a larger item up, until it is smaller
    * than its parent. */
  @tailrec
  private def siftUp(index: Int): Unit = {
    if (hasParent(index)) {
      val parentIndex = parent(index)
      val thisValue = _values(index)
      val parentValue = _values(parentIndex)
      if (ordering.compare(thisValue, parentValue) > 0) {
        swap(index, parentIndex)
        siftUp(parentIndex)
      }
    }
  }

  /** Maintains the heap invariant by moving a smaller item up, until it is larger
    * than all of its children. */
  @tailrec
  private def siftDown(index: Int): Unit = {
    val thisValue = _values(index)
    if (hasLeft(index)) {
      val maxIndex = indexOfMaxChild(index)
      val maxValue = _values(maxIndex)
      if (ordering.compare(thisValue, maxValue) < 0) {
        swap(index, maxIndex)
        siftDown(maxIndex)
      }
    }
  }

  private def siftUpOrDown(index: Int): Unit = {
    siftUp(index)
    siftDown(index)
  }

  /** Removes an element from the heap, replaces it with the last element,
    * and fixes the position of the replacement element to keep the heap invariant. */
  private def removeAt(index: Int): Unit = {
    _size -= 1
    if (index != _size) {
      move(_size, index)
      siftUpOrDown(index)
    }
    clear(_size)
  }

  /** Updates a value and moves it up or down in the heap. */
  private def update(pos: Int, value: V): Unit = {
    val index = _indexes(pos)
    _values(index) = value
    siftUpOrDown(index)
  }

  /** Adds a new entry to the end of the heap and updates
    * the indexes hash-table. */
  private def add(pos: Int, key: K, value: V): Unit = {
    if (_size == capacity)
      throw new IllegalStateException(
        s"Cannot add a new item ($key -> $value) to a PriorityMap that reached its maximum capacity $capacity")
    val index = _size
    _size += 1
    _keys(index) = key
    _values(index) = value
    _indexes(pos) = index
    siftUp(index)
  }

  /** Adds or updates a map entry.
    * Complexity: O(log n) average, O(1) optimistic. */
  def put(key: K, value: V): Unit = {
    val pos = find(key)
    if (_indexes(pos) < 0)
      add(pos, key, value)
    else
      update(pos, value)
  }

  /** Returns a value associated with the given key.
    * If the key does not exist, throws NoSuchElementException.
    * If you know the key does exist, this method is preferred over
    * the [[get]] method, because it doesn't allocate an `Option` object.
    * Complexity: O(1). */
  def apply(key: K): V = {
    val pos = find(key)
    val index = _indexes(pos)
    if (index < 0)
      throw new NoSuchElementException(s"Key not found $key")
    _values(index)
  }

  /** Returns a value associated with the given key.
    * If the key does not exist, returns None.
    * Complexity: O(1). */
  def get(key: K): Option[V] = {
    val pos = find(key)
    val index = _indexes(pos)
    if (index < 0)
      None
    else
      Some(_values(index))
  }

  /** Returns true if the map contains given key. */
  def contains(key: K): Boolean =
    _indexes(find(key)) >= 0

  /** Removes a key and reorders remaining items.
    * If the key does not exist, does nothing.
    * Returns true if key existed.
    * Complexity: O(log n) average, O(1) optimistic. */
  def remove(key: K): Boolean = {
    val pos = find(key)
    val index = _indexes(pos)
    if (index >= 0) {
      removeIndex(pos)
      removeAt(index)
      true
    }
    else
      false
  }

  private def checkNonEmpty(): Unit =
    if (_size == 0)
      throw new NoSuchElementException("Requested head of an empty PriorityMap")

  /** Returns the key associated with the largest value.
    * Complexity: O(1). */
  def headKey: K = {
    checkNonEmpty()
    _keys(0)
  }

  /** Returns the largest value.
    * Complexity: O(1). */
  def headValue: V = {
    checkNonEmpty()
    _values(0)
  }

  /** Useful for iterating the map. */
  def keys: IndexedSeq[K] =
    _keys.take(size)

  /** Useful for iterating the map */
  def values: IndexedSeq[V] =
    _values.take(size)

  override def toString: String = {
    "PriorityHashMap(" + _keys.zip(_values).take(size).mkString(",") + ")"
  }
}
