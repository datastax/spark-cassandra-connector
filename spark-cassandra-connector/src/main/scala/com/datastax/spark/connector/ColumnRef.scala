package com.datastax.spark.connector

import scala.language.implicitConversions

/** A column that can be selected from CQL results set by name. */
sealed trait ColumnRef  {

  /** Returns the column name which this selection bases on.
    * In case of a function, such as `ttl` or `writetime`,
    * it returns the column name passed to that function. */
  def columnName: String

  /** Returns the name of the column to be used by the user in the RDD item object.
    * If the column is selected into a CassandraRow, this name should be used to get the
    * column value. Also this name will be matched to an object property by column mappers. */
  def selectedAs: String

  /** Returns a CQL phrase which has to be passed to the
    * `SELECT` clause with appropriate quotation marks. */
  def cql: String

  /** Returns a name of the value of this column in the CQL result set.
    * Used when retrieving values form the CQL result set.*/
  def cqlValueName: String
}

/** References a column by name. */
case class ColumnName(columnName: String, alias: Option[String] = None) extends ColumnRef {
  override val cql = s""""$columnName""""
  override def cqlValueName = columnName
  override def selectedAs = alias.getOrElse(columnName)
  override def toString: String = columnName

  def as(alias: String) = copy(alias = Some(alias))
}

/** References TTL of a column. */
case class TTL(columnName: String, alias: Option[String] = None) extends ColumnRef {
  override val cql = s"""TTL("$columnName")"""
  override val cqlValueName = s"ttl($columnName)"
  override def selectedAs = alias.getOrElse(cqlValueName)
  override def toString: String = cqlValueName

  def as(alias: String) = copy(alias = Some(alias))
}

/** References write time of a column. */
case class WriteTime(columnName: String, alias: Option[String] = None) extends ColumnRef {
  override val cql = s"""WRITETIME("$columnName")"""
  override val cqlValueName = s"writetime($columnName)"
  override def selectedAs = alias.getOrElse(cqlValueName)
  override def toString: String = cqlValueName

  def as(alias: String) = copy(alias = Some(alias))
}

/** References a row count value returned from SELECT count(*)  */
case object RowCountRef extends ColumnRef {
  override def selectedAs: String = "count"
  override def columnName: String = "count"
  override def cqlValueName: String = "count"
  override def cql: String = "count(*)"
}
