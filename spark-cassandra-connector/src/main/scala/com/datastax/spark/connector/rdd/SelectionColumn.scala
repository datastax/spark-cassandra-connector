package com.datastax.spark.connector.rdd

sealed trait SelectionColumn {
  def columnName: String

  def cql: String

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
