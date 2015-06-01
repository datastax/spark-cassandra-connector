package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.ColumnRef

/** Transforms a Cassandra Java driver `Row` into high-level row representation, e.g. a tuple
  * or a user-defined case class object. The target type `T` must be serializable. */
trait RowReader[T] extends Serializable {

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    * @param row row fetched from Cassandra
    * @param columnNames column names available in the `row`
    * @param protocolVersion java driver protocol version to be used for deserialization */
  def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): T

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  def neededColumns: Option[Seq[ColumnRef]]

  /** This method should be implemented by those row readers which reads fields in the consecutive
    * positions from a CassandraRow. When a row reader implements it so that it returns a non-empty,
    * it denotes the number of columns this reader moves the column cursor forward for compound row
    * readers (such as [[com.datastax.spark.connector.rdd.reader.KeyValueRowReader]]). */
  def consumedColumns: Option[Int] = None

}
