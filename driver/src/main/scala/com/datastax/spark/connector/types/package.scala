package com.datastax.spark.connector

import scala.util.Try

/** Offers type conversion magic, so you can receive Cassandra column values in a form you like the most.
  * Simply specify the type you want to use on the Scala side, and the column value will be converted automatically.
  * Works also with complex objects like collections. */
package object types {

  /** Spark's GenericRowWithScheme prototype created to avoid dependency on Spark.
    * FIXME: Any ideas how to get rid of this type are welcome. */
  type GenericRowWithSchemePrototype = {
    def fieldIndex(columnName: String): Int
    def get(index: Int): Any
    def toSeq: Seq[Any]
  }

  object IsGenericRowWithSchemePrototype {
    def containsMethod(x: AnyRef, name: String, params: Array[java.lang.Class[_]]): Boolean =
      Try(x.getClass.getMethod(name, params: _*)).isSuccess

    def unapply(foo: AnyRef): Option[GenericRowWithSchemePrototype] = {
      if (containsMethod(foo, "fieldIndex", Array(classOf[String])) &&
        containsMethod(foo, "get", Array(classOf[Int])) &&
        containsMethod(foo, "toSeq", Array())) {
        Some(foo.asInstanceOf[GenericRowWithSchemePrototype])
      } else None
    }
  }

  /** Makes sure the sequence does not contain any lazy transformations.
    * This guarantees that if T is Serializable, the collection is Serializable. */
  def unlazify[T](seq: IndexedSeq[T]): IndexedSeq[T] = IndexedSeq(seq: _*)

}
