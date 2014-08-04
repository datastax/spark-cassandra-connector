package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.Row

/** Transforms a Cassandra Java driver `Row` into high-level row representation, e.g. a tuple
  * or a user-defined case class object. The target type `T` must be serializable. */
trait RowReader[T] extends Serializable {

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    * @param row row fetched from Cassandra
    * @param columnNames column names available in the `row` */
  def read(row: Row, columnNames: Array[String]): T

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  def columnNames: Option[Seq[String]]

  /** The number of columns that need to be fetched from C*. */
  def columnCount: Option[Int]
}

