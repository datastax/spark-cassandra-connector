package com.datastax.spark.connector.rdd

sealed trait SelectionColumn {
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

case class PlainSelectionColumn(columnName: String) extends SelectionColumn {
  val cql = s""""$columnName""""
  val selectedAs = columnName

  override def toString: String = selectedAs
}

case class TTLColumn(columnName: String) extends SelectionColumn {
  val cql = s"""TTL("$columnName")"""
  val selectedAs = s"ttl($columnName)"

  override def toString: String = selectedAs
}

case class WriteTimeColumn(columnName: String) extends SelectionColumn {
  val cql = s"""WRITETIME("$columnName")"""
  val selectedAs = s"writetime($columnName)"

  override def toString: String = selectedAs
}
