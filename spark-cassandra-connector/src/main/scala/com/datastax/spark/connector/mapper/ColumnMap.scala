package com.datastax.spark.connector.mapper

/** Unambiguous reference to a column in the query result set row. */
sealed trait ColumnRef

/** References a column by name. */
case class NamedColumnRef(name: String) extends ColumnRef

/** References a column by its index in the row. Useful for tuples. */
case class IndexedColumnRef(index: Int) extends ColumnRef

/** Associates constructor parameters and property accessors with table columns */
trait ColumnMap extends Serializable {
  def constructor: Seq[ColumnRef]
  def getters: Map[String, ColumnRef]
  def setters: Map[String, ColumnRef]
}

case class SimpleColumnMap(
  constructor: Seq[ColumnRef],
  getters: Map[String, ColumnRef],
  setters: Map[String, ColumnRef]) extends ColumnMap

