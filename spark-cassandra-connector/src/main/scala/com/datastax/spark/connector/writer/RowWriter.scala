package com.datastax.spark.connector.writer

import com.datastax.spark.connector.ColumnRef


/** `RowWriter` knows how to extract column names and values from custom row objects
  * and how to convert them to values that can be written to Cassandra.
  * `RowWriter` is required to apply any user-defined data type conversion. */
trait RowWriter[T] extends Serializable {

  /**
    * ColumnRef objects for all the columns that this `RowWriter` will write. Column Names are used
    * to construct appropriate INSERT or UPDATE statements and metadata controls more obscure
    * binding behavior (No Null/ Unbound TTL on C* 2.1 )
    */
  def columnRefs: IndexedSeq[ColumnRef]

  lazy val columnNames: IndexedSeq[String] = columnRefs.map(_.columnName)

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  def readColumnValues(data: T, buffer: Array[Any])

}
