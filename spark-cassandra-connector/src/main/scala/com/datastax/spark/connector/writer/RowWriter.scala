package com.datastax.spark.connector.writer


/** `RowWriter` knows how to extract column names and values from custom row objects
  * and how to convert them to values that can be written to Cassandra.
  * `RowWriter` is required to apply any user-defined data type conversion. */
trait RowWriter[T] extends Serializable {

  /** List of columns this `RowWriter` is going to write.
    * Used to construct appropriate INSERT or UPDATE statement. */
  def columnNames: Seq[String]

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  def readColumnValues(data: T, buffer: Array[Any])

}
