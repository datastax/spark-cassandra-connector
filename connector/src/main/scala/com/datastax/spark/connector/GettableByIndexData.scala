package com.datastax.spark.connector

import com.datastax.spark.connector.types.TypeConverter.StringConverter

trait GettableByIndexData extends Serializable {

  def columnValues: IndexedSeq[AnyRef]

  /** Returns a column value by index without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver,
    * is implementation defined and may change in the future.
    * Cassandra nulls are returned as Scala nulls. */
  def getRaw(index: Int): AnyRef = columnValues(index)

  /** Total number of columns in this row. Includes columns with null values. */
  def length = columnValues.size

  /** Total number of columns in this row. Includes columns with null values. */
  def size = columnValues.size

  /** Returns true if column value is Cassandra null */
  def isNullAt(index: Int): Boolean =
    columnValues(index) == null
  
  /** Displays the content in human readable form, including the names and values of the columns */
  def dataAsString: String =
    columnValues
      .map(StringConverter.convert)
      .mkString("(", ", ", ")")

  override def toString =
    dataAsString

  override def equals(o: Any): Boolean = o match {
    case that: GettableByIndexData if this.columnValues == that.columnValues => true
    case _ => false
  }

  override def hashCode: Int =
    columnValues.hashCode()
}
