package com.datastax.spark.connector

import scala.language.implicitConversions

/** Unambiguous reference to a column in the query result set row. */
sealed trait ColumnRef

/** A column that can be selected from CQL results set by name */
sealed trait SelectableColumnRef extends ColumnRef {
  /** Returns a CQL phrase which has to be passed to the `SELECT` clause with appropriate quotation
    * marks. */
  def cql: String

  /** Returns a name of the selection as it is seen in the result set. Most likely this is going to be
    * used when providing custom column name to field name mapping. */
  def selectedFromCassandraAs: String

  /** Returns the name of the column to be used by the user in the RDD item object.
    * If the column is selected into a CassandraRow, this name should be used to get the
    * column value. Also this name will be matched to an object property by column mappers */
  def selectedAs: String
}

object SelectableColumnRef {
  def unapply(columnRef: SelectableColumnRef) = Some(columnRef.selectedFromCassandraAs)
}

/** A selectable column based on a real, non-virtual column with a name in the table */
sealed trait NamedColumnRef extends SelectableColumnRef {
  /** Returns the column name which this selection bases on. In case of a function, such as `ttl` or
    * `writetime`, it returns the column name passed to that function. */
  def columnName: String
}

object NamedColumnRef {
  def unapply(columnRef: NamedColumnRef) = Some((columnRef.columnName, columnRef.selectedFromCassandraAs))
}

/** References a column by name. */
case class ColumnName(columnName: String, alias: Option[String] = None) extends NamedColumnRef {
  val cql = s""""$columnName""""
  val selectedFromCassandraAs = columnName
  def selectedAs = alias.getOrElse(columnName)

  def as(alias: String) = copy(alias = Some(alias))

  override def toString: String = selectedFromCassandraAs
}

case class TTL(columnName: String, alias: Option[String] = None) extends NamedColumnRef {
  val cql = s"""TTL("$columnName")"""
  val selectedFromCassandraAs = s"ttl($columnName)"
  def selectedAs = alias.getOrElse(selectedFromCassandraAs)

  def as(alias: String) = copy(alias = Some(alias))

  override def toString: String = selectedFromCassandraAs
}

case class WriteTime(columnName: String, alias: Option[String] = None) extends NamedColumnRef {
  val cql = s"""WRITETIME("$columnName")"""
  val selectedFromCassandraAs = s"writetime($columnName)"
  def selectedAs = alias.getOrElse(selectedFromCassandraAs)

  def as(alias: String) = copy(alias = Some(alias))

  override def toString: String = selectedFromCassandraAs
}

case object RowCountRef extends SelectableColumnRef {
  override def selectedFromCassandraAs: String = "count"
  override def selectedAs: String = "count"
  override def cql: String = "count(*)"
}

/** References a column by its index in the row. Useful for tuples. */
case class ColumnIndex(columnIndex: Int) extends ColumnRef
