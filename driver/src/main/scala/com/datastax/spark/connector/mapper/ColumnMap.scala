package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnRef

/** A column map for saving objects to Cassandra.
  * Lists available getters. */
trait ColumnMapForWriting extends Serializable {
  /** Maps a getter method name to a column reference */
  def getters: Map[String, ColumnRef]
}

/** A column map for reading objects from Cassandra.
  * Describes object's constructor and setters. */
trait ColumnMapForReading extends Serializable {

  /** A sequence of column references associated with parameters of the main constructor.
    * If the class contains multiple constructors, the main constructor is assumed to be the one with the
    * highest number of parameters. Multiple constructors with the same number of parameters are not allowed. */
  def constructor: Seq[ColumnRef]

  /** Maps a setter method name to a column reference */
  def setters: Map[String, ColumnRef]

  /** Whether Java nulls are allowed to be directly set as object properties.
    * This is desired for compatibility with Java classes, but in Scala, we have Option,
    * therefore we need to fail fast if one wants to assign a Cassandra null
    * value to a non-optional property. */
  def allowsNull: Boolean
}

case class SimpleColumnMapForWriting(getters: Map[String, ColumnRef]) extends ColumnMapForWriting

case class SimpleColumnMapForReading(
  constructor: Seq[ColumnRef],
  setters: Map[String, ColumnRef],
  allowsNull: Boolean = false) extends ColumnMapForReading
