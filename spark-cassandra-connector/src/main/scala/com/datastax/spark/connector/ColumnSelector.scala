package com.datastax.spark.connector

sealed trait ColumnSelector
case object AllColumns extends ColumnSelector
case class SomeColumns(columns: String*) extends ColumnSelector

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns: _*)
}


