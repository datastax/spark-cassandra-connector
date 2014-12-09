package com.datastax.spark.connector.mapper

/** Unambiguous reference to a column in the query result set row. */
sealed trait ColumnRef

sealed trait IndexedByNameColumnRef extends ColumnRef {
  /** Returns the column name which this selection bases on. In case of a function, such as `ttl` or
    * `writetime`, it returns the column name passed to that function. */
  def name: String

  /** Returns a CQL phrase which has to be passed to the `SELECT` clause with appropriate quotation
    * marks. */
  def cql: String

  /** Returns a name of the selection as it is seen in the result set. Most likely this is going to be
    * used when providing custom column name to field name mapping. */
  def selectedAs: String
}

object IndexedByNameColumnRef {
  def unapply(columnRef: IndexedByNameColumnRef) = Some((columnRef.name, columnRef.selectedAs))
}

/** References a column by name. */
case class NamedColumnRef(name: String) extends IndexedByNameColumnRef {
  val cql = s""""$name""""
  val selectedAs = name

  override def toString: String = selectedAs
}

case class TTL(name: String) extends IndexedByNameColumnRef {
  val cql = s"""TTL("$name")"""
  val selectedAs = s"ttl($name)"

  override def toString: String = selectedAs
}

case class WriteTime(name: String) extends IndexedByNameColumnRef {
  val cql = s"""WRITETIME("$name")"""
  val selectedAs = s"writetime($name)"

  override def toString: String = selectedAs
}

/** References a column by its index in the row. Useful for tuples. */
case class IndexedColumnRef(index: Int) extends ColumnRef

/** Associates constructor parameters and property accessors with table columns */
trait ColumnMap extends Serializable {
  def constructor: Seq[ColumnRef]
  def getters: Map[String, ColumnRef]
  def setters: Map[String, ColumnRef]
  def allowsNull: Boolean
}

case class SimpleColumnMap(
  constructor: Seq[ColumnRef],
  getters: Map[String, ColumnRef],
  setters: Map[String, ColumnRef],
  allowsNull: Boolean = false) extends ColumnMap

