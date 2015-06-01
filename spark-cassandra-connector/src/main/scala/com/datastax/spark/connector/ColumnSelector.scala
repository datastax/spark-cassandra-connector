package com.datastax.spark.connector

import scala.language.implicitConversions

import com.datastax.spark.connector.mapper.IndexedByNameColumnRef

sealed trait ColumnSelector
case object AllColumns extends ColumnSelector
case class SomeColumns(@transient _columns: IndexedByNameColumnRef*) extends ColumnSelector {
  // In previous minor releases this method accepts a vararg of Strings. Varargs in scala are
  // resolved as Seq and the type parameter is erased. Therefore we can change the type parameter
  // while keeping the backward binary compatibility - that is, the applications which were
  // compiled against the previous minor release, which pass vararg of Strings are still able
  // to work.
  // In order to make it work, we need to explicitly cast _columns to Seq[AnyRef] before invoking
  // any command on this sequence. Then we check what is the real type of each entry in the sequence,
  // and convert it to IndexedByNameColumnRef if necessary.
  val columns = _columns.asInstanceOf[Seq[AnyRef]].map {
    case ref: IndexedByNameColumnRef ⇒ ref
    case str ⇒ str.toString: IndexedByNameColumnRef
  }
}

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: IndexedByNameColumnRef): _*)

  def unapply(sc: SomeColumns): Option[Seq[IndexedByNameColumnRef]] = {
    Some(sc.columns)
  }
}


