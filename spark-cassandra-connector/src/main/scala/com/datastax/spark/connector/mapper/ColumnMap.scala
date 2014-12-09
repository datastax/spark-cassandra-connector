package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnRef

/** Associates constructor parameters and property accessors with table columns */
trait ColumnMap extends Serializable {
  def constructor: Seq[ColumnRef]

  def getters: Map[String, ColumnRef]

  def setters: Map[String, ColumnRef]

  def allowsNull: Boolean
}

case class SimpleColumnMap(constructor: Seq[ColumnRef],
                           getters: Map[String, ColumnRef],
                           setters: Map[String, ColumnRef],
                           allowsNull: Boolean = false) extends ColumnMap

