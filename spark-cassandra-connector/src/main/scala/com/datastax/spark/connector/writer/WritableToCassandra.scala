package com.datastax.spark.connector.writer

import scala.collection.immutable

trait WritableToCassandra[T] {
  import WritableColumns._

  /**
   * Saves the data from `RDD` to a Cassandra table.
   * By default, it saves all properties that have corresponding Cassandra columns.
   *
   * Example:
   * {{{
   *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
   *   CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
   * }}}
   *
   * {{{
   *   case class WordCount(word: String, count: Int, other: String)
   *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
   * }}}
   *
   * By default, the underlying RDD class must provide data for all columns:
   * {{{
   *   rdd.saveToCassandra("test", "words")
   * }}}
   *
   * By default, writes are performed at ConsistencyLevel.ONE in order to leverage data-locality and minimize network traffic.
   * This write consistency level is controlled by the following property:
   *   - spark.cassandra.output.consistency.level: consistency level for RDD writes, string matching the ConsistencyLevel enum name.
   *
   * @param keyspaceName the name of the Keyspace to use
   *
   * @param tableName the name of the Table to use
   */
  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T])


  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   *
   * {{{
   *   rdd.saveToCassandra("test", "words", ColumnNames(Set("word", "count")))
   * }}}
   *
   * @param keyspaceName the name of the Keyspace to use
   *
   * @param tableName the name of the Table to use
   *
   * @param columnNames The list of column names to save data to.
   *                Uses only the unique column names, and you must select at least all primary key
   *                columns. All other fields are discarded. Non-selected property/column names are left unchanged.
   */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: ColumnNames)(implicit rwf: RowWriterFactory[T])

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names with an additional batch size.
   *
   * {{{
   *   rdd.saveToCassandra("test", "words", ColumnNames(Set("word", "count")), size)
   * }}}
   *
   *
   * @param keyspaceName the name of the Keyspace to use
   *
   * @param tableName the name of the Table to use
   *
   * @param columnNames The list of columns to save data to.
   *                Uses only the unique column names, and you must select at least all primary key
   *                columns. All other fields are discarded. Non-selected property/column names are left unchanged.
   *
   * @param batchSize The batch size. By default, if the batch size is unspecified, the right amount
   *                  is calculated automatically according the average row size. Specify explicit value
   *                  here only if you find automatically tuned batch size doesn't result in optimal performance.
   *                  Larger batches raise memory use by temporary buffers and may incur larger GC pressure on the server.
   *                  Small batches would result in more round trips and worse throughput. Typically sending a few kilobytes
   *                  of data per every batch is enough to achieve good performance.
   */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: ColumnNames, batchSize: Int)(implicit rwf: RowWriterFactory[T])

}

object WritableColumns {

  /** Base marker trait for columns to write to Cassandra. */
  trait WritableColumns extends Serializable
  
  /** INTERNAL API.
    * Command to write all columns to Cassandra.
    */
  private[connector] case object AllColumns extends WritableColumns
  
  /** Command to write only specified columns to Cassandra.
    *
    * @param names the column names subset to use. Uses only the unique column names,
    *              and you must select at least all primary key columns.
    */
  case class ColumnNames(names: immutable.Set[String]) extends WritableColumns {
    require(names.nonEmpty, "'names' must not be empty.")
  }
}
