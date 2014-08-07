package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ BoundStatement, PreparedStatement }

/**
 * `RowWriter` knows how to write an object to Cassandra using the Java Cassandra driver.
 */
trait RowWriter[T] extends Serializable {

  /**
   * Extracts column values from `data` object and binds them to the given statement.
   * Variables of the prepared statement are named the same as column names to be saved.
   * This method must not rely on any particular order of variables.
   */
  def bind(data: T, stmt: PreparedStatement): BoundStatement

  /**
   * Estimates serialized size in bytes of a data object.
   * Used for grouping statements into batches.
   */
  def estimateSizeInBytes(data: T): Int

  /**
   * List of columns this `RowWriter` is going to write.
   * Used to construct appropriate INSERT or UPDATE statement.
   */
  def columnNames: Seq[String]

}
