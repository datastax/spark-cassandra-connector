package com.datastax.spark.connector.types

import scala.collection.mutable
import scala.collection.immutable.{TreeMap, TreeSet}

/** Serves the same purpose as standard Scala `CanBuildFrom`, however this one is `Serializable`. */
trait CanBuildFrom[-Elem, To] extends Serializable {

  def apply(): mutable.Builder[Elem, To]

  // it is safe to upcast. because CanBuildFrom is in fact covariant in To. However we cannot mark it
  // covariant, because that would trigger implicit resolution ambiguities and
  // the workaround for them is much more complex than this "asInstanceOf" hack.
  def upcast[T >: To] = this.asInstanceOf[CanBuildFrom[Elem, T]]
}

object CanBuildFrom {

  implicit def iterableCanBuildFrom[T] = new CanBuildFrom[T, Iterable[T]] {
    override def apply() = Iterable.newBuilder[T]
  }

  implicit def seqCanBuildFrom[T] = new CanBuildFrom[T, Seq[T]] {
    override def apply() = Seq.newBuilder[T]
  }

  implicit def indexedSeqCanBuildFrom[T] = new CanBuildFrom[T, IndexedSeq[T]] {
    override def apply() = IndexedSeq.newBuilder[T]
  }

  implicit def vectorCanBuildFrom[T] = new CanBuildFrom[T, Vector[T]] {
    override def apply() = Vector.newBuilder[T]
  }

  implicit def listCanBuildFrom[T] = new CanBuildFrom[T, List[T]] {
    override def apply() = List.newBuilder[T]
  }

  implicit def setCanBuildFrom[T] = new CanBuildFrom[T, Set[T]] {
    override def apply() = Set.newBuilder[T]
  }

  implicit def treeSetCanBuildFrom[T : Ordering] = new CanBuildFrom[T, TreeSet[T]] {
    override def apply() = TreeSet.newBuilder[T](implicitly[Ordering[T]])
  }

  implicit def mapCanBuildFrom[K, V] = new CanBuildFrom[(K, V), Map[K, V]] {
    override def apply() = Map.newBuilder[K, V]
  }

  implicit def treeMapCanBuildFrom[K : Ordering, V] = new CanBuildFrom[(K, V), TreeMap[K, V]] {
    override def apply() = TreeMap.newBuilder[K, V]
  }

  implicit def javaArrayListCanBuildFrom[T] = new CanBuildFrom[T, java.util.ArrayList[T]] {
    override def apply() = new scala.collection.mutable.Builder[T, java.util.ArrayList[T]]() {
      val list = new java.util.ArrayList[T]()
      override def +=(elem: T) = { list.add(elem); this }
      override def result() = list
      override def clear() = list.clear()
    }
  }

  implicit def javaListCanBuildFrom[T] =
    javaArrayListCanBuildFrom[T].upcast[java.util.List[T]]

  implicit def javaHashSetCanBuildFrom[T] = new CanBuildFrom[T, java.util.HashSet[T]] {
    override def apply() = new scala.collection.mutable.Builder[T, java.util.HashSet[T]]() {
      val set = new java.util.HashSet[T]()
      override def +=(elem: T) = { set.add(elem); this }
      override def result() = set
      override def clear() = set.clear()
    }
  }

  implicit def javaSetCanBuildFrom[T] =
    javaHashSetCanBuildFrom[T].upcast[java.util.Set[T]]

  implicit def javaHashMapCanBuildFrom[K, V] = new CanBuildFrom[(K, V), java.util.HashMap[K, V]] {
    override def apply() = new scala.collection.mutable.Builder[(K, V), java.util.HashMap[K, V]]() {
      val map = new java.util.HashMap[K, V]()
      override def +=(elem: (K, V)) = { map.put(elem._1, elem._2); this }
      override def result() = map
      override def clear() = map.clear()
    }
  }

  implicit def javaMapCanBuildFrom[K, V] =
    javaHashMapCanBuildFrom[K, V].upcast[java.util.Map[K, V]]
}