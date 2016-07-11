package com.datastax.spark.connector.writer

import com.datastax.spark.connector.ColumnRef


/** `RowWriter` knows how to extract column names and values from custom row objects
  * and how to convert them to values that can be written to Cassandra.
  * `RowWriter` is required to apply any user-defined data type conversion. */
trait RowWriter[T] extends Serializable {

  /**
    * ColumnRef objects for all the columns that this `RowWriter` will write.
    */
  def columnRefs: Seq[ColumnRef]

  /** List of columns this `RowWriter` is going to write.
    * Used to construct appropriate INSERT or UPDATE statement. */
  final lazy val columnNames: Seq[String] = columnRefs.map(_.columnName)

  /**
    * Mapping back to column refs from column names
    */
  final lazy val columnNameToRef: Map[String, ColumnRef] = columnRefs
    .map( ref => (ref.columnName, ref))
    .toMap

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  def readColumnValues(data: T, buffer: Array[Any])

}
