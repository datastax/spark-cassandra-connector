package com.datastax.spark.connector

import com.datastax.spark.connector.cql.{TableDef, CassandraConnector}
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.rdd.SpannedRDD
import com.datastax.spark.connector.writer._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  override val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columns: ColumnSelector = AllColumns,
                      writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                     (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                      rwf: RowWriterFactory[T]): Unit = {

    val writer = TableWriter(connector, keyspaceName, tableName, columns, writeConf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Saves the data from `RDD` to a new table defined by the given `TableDef`.
   *
   * First it creates a new table with all columns from the `TableDef`
   * and then it saves RDD content in the same way as [[saveToCassandra]].
   * The table must not exist prior to this call.
   *
   * @param table table definition used to create a new table
   * @param columns Selects the columns to save data to.
   *                Uses only the unique column names, and you must select at least all primary key
   *                 columns. All other fields are discarded. Non-selected property/column names are left unchanged.
   *                 This parameter does not affect table creation.
   * @param writeConf additional configuration object allowing to set consistency level, batch size, etc.
   * @param connector optional, implicit connector to Cassandra
   * @param rwf factory for obtaining the row writer to be used to extract column values
   *            from items of the `RDD`
   */
  def saveAsCassandraTableEx(table: TableDef,
                             columns: ColumnSelector = AllColumns,
                             writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                            (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                             rwf: RowWriterFactory[T]): Unit = {

    connector.withSessionDo(session => session.execute(table.cql))
    saveToCassandra(table.keyspaceName, table.tableName, columns, writeConf)
  }

  /**
   * Saves the data from `RDD` to a new table with definition taken from the
   * `ColumnMapper` for this class.
   *
   * @param keyspaceName keyspace where to create a new table
   * @param tableName name of the table to create; the table must not exist
   * @param columns Selects the columns to save data to.
   *                Uses only the unique column names, and you must select at least all primary key
   *                 columns. All other fields are discarded. Non-selected property/column names are left unchanged.
   *                 This parameter does not affect table creation.
   * @param writeConf additional configuration object allowing to set consistency level, batch size, etc.
   * @param connector optional, implicit connector to Cassandra
   * @param rwf factory for obtaining the row writer to be used to extract column values
   *            from items of the `RDD`
   * @param columnMapper a column mapper determining the definition of the table
   */
  def saveAsCassandraTable(keyspaceName: String,
                           tableName: String,
                           columns: ColumnSelector = AllColumns,
                           writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                          (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                           rwf: RowWriterFactory[T],
                           columnMapper: ColumnMapper[T]): Unit = {

    val table = TableDef.fromType[T](keyspaceName, tableName)
    saveAsCassandraTableEx(table, columns, writeConf)
  }

  /** Applies a function to each item, and groups consecutive items having the same value together.
    * Contrary to `groupBy`, items from the same group must be already next to each other in the
    * original collection. Works locally on each partition, so items from different
    * partitions will never be placed in the same group.*/
  def spanBy[U](f: (T) => U): RDD[(U, Iterable[T])] =
    new SpannedRDD[U, T](rdd, f)

}
