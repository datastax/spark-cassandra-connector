package com.datastax.spark.connector.writer

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext

abstract class WritableToCassandra[T] {

  def sparkContext: SparkContext

  private[connector] lazy val connector = CassandraConnector(sparkContext.getConf)

  /**
   * Saves the data from [[org.apache.spark.rdd.RDD RDD]] to a Cassandra table.
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
   * By default, writes are performed at ConsistencyLevel.LOCAL_QUORUM.
   * This write consistency level is controlled by the following property:
   *   - spark.cassandra.output.consistency.level: consistency level for RDD writes, string matching the ConsistencyLevel enum name.
   *
   * @param keyspaceName the name of the Keyspace to use
   * @param tableName the name of the Table to use
   * @param columnNames The list of column names to save data to.
   *                Uses only the unique column names, and you must select at least all primary key
   *                columns. All other fields are discarded. Non-selected property/column names are left unchanged.
   * @param writeConf additional configuration object allowing to set consistency level, batch size, etc.
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: ColumnSelector,
                      writeConf: WriteConf)
                     (implicit connector: CassandraConnector, rwf: RowWriterFactory[T])

}
