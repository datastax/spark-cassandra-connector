package com.datastax.driver.spark.mapper

sealed trait ColumnRef
case class NamedColumnRef(name: String) extends ColumnRef
case class IndexedColumnRef(index: Int) extends ColumnRef

trait ColumnMap extends Serializable {
  def constructor: Seq[ColumnRef]
  def getters: Map[String, ColumnRef]
  def setters: Map[String, ColumnRef]
}

case class SimpleColumnMap(
  constructor: Seq[ColumnRef],
  getters: Map[String, ColumnRef],
  setters: Map[String, ColumnRef]) extends ColumnMap

