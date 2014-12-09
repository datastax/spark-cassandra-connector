package com.datastax.spark.connector

import scala.language.implicitConversions

/** Unambiguous reference to a column in the query result set row. */
sealed trait ColumnRef

sealed trait NamedColumnRef extends ColumnRef {
  /** Returns the column name which this selection bases on. In case of a function, such as `ttl` or
    * `writetime`, it returns the column name passed to that function. */
  def columnName: String

  /** Returns a CQL phrase which has to be passed to the `SELECT` clause with appropriate quotation
    * marks. */
  def cql: String

  /** Returns a name of the selection as it is seen in the result set. Most likely this is going to be
    * used when providing custom column name to field name mapping. */
  def selectedAs: String
}

object NamedColumnRef {
  def unapply(columnRef: NamedColumnRef) = Some((columnRef.columnName, columnRef.selectedAs))
}

/** References a column by name. */
case class ColumnName(columnName: String) extends NamedColumnRef {
  val cql = s""""$columnName""""
  val selectedAs = columnName

  override def toString: String = selectedAs
}

case class TTL(columnName: String) extends NamedColumnRef {
  val cql = s"""TTL("$columnName")"""
  val selectedAs = s"ttl($columnName)"

  override def toString: String = selectedAs
}

case class WriteTime(columnName: String) extends NamedColumnRef {
  val cql = s"""WRITETIME("$columnName")"""
  val selectedAs = s"writetime($columnName)"

  override def toString: String = selectedAs
}

/** References a column by its index in the row. Useful for tuples. */
case class ColumnIndex(columnIndex: Int) extends ColumnRef
