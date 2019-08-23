package com.datastax.spark.connector

import scala.util.Try

/** Offers type conversion magic, so you can receive Cassandra column values in a form you like the most.
  * Simply specify the type you want to use on the Scala side, and the column value will be converted automatically.
  * Works also with complex objects like collections. */
package object types {

  /** Makes sure the sequence does not contain any lazy transformations.
    * This guarantees that if T is Serializable, the collection is Serializable. */
  def unlazify[T](seq: IndexedSeq[T]): IndexedSeq[T] = IndexedSeq(seq: _*)

}
