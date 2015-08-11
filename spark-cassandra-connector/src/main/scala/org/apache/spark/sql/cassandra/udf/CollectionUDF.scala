package org.apache.spark.sql.cassandra.udf

object CollectionUDF {

  def collectionSize(s: Seq[_]): Int = {
    s.size
  }
  def collectionSize(s: Set[_]): Int = {
    s.size
  }

  def collectionSize(m: Map[_,_]): Int = {
    m.size
  }

  def mapKeys[T](map: Map[T, _]): Seq[T] = {
    map.keySet.toSeq
  }

  def mapValues[T](map: Map[_, T]): Seq[T] = {
    map.values.toList.toSeq
  }

  def mapKeyContains[T](map: Map[T, _], key: T): Boolean = {
    map.keySet.contains(key)
  }

  def mapValueContains[T](map: Map[_, T], value: T): Boolean = {
    map.values.exists(_ == value)
  }

  def seqContains[T](seq: Seq[T], value: T): Boolean = {
    seq.contains(value)
  }

  def setContains[T](set: Set[T], value: T): Boolean = {
    set.contains(value)
  }

  def sortSeq[T: Ordering](seq: Seq[T]): Seq[T] = {
    seq.sorted
  }

  def mapValue[K, V](map: Map[K, V], key: K): V = {
    map(key)
  }
}
