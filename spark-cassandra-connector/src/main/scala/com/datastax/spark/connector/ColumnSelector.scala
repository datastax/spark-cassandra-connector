package com.datastax.spark.connector

import com.datastax.spark.connector.rdd.SelectionColumn

sealed trait ColumnSelector
case object AllColumns extends ColumnSelector
case class SomeColumns(columns: SelectionColumn*) extends ColumnSelector

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: SelectionColumn): _*)
}


