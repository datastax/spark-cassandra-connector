package com.datastax.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{RowWriterFactory, TableWriter}
import com.datastax.driver.core.ConsistencyLevel
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {

  private lazy val batchSizeInRowsStr = rdd.sparkContext.getConf.get(
    "spark.cassandra.output.batch.size.rows", "auto")

  private lazy val batchSizeInBytes = rdd.sparkContext.getConf.getInt(
    "spark.cassandra.output.batch.size.bytes", TableWriter.DefaultBatchSizeInBytes)

  private lazy val outputConsistencyLevel = ConsistencyLevel.valueOf(
    rdd.sparkContext.getConf.get("spark.cassandra.output.consistency.level", ConsistencyLevel.LOCAL_ONE.name))

  private lazy val batchSizeInRows = {
    val Number = "([0-9]+)".r
    batchSizeInRowsStr match {
      case "auto" => None
      case Number(x) => Some(x.toInt)
      case other =>
        throw new ConfigurationException(
          s"Invalid value of spark.cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
    }
  }

  private lazy val writeParallelismLevel = rdd.sparkContext.getConf.getInt(
    "spark.cassandra.output.concurrent.writes", TableWriter.DefaultParallelismLevel)

  private lazy val connector = CassandraConnector(rdd.sparkContext.getConf)

  /** Saves the data from `RDD` to a Cassandra table.
    * Saves all properties that have corresponding Cassandra columns.
    * The underlying RDD class must provide data for all columns.
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
    *   rdd.saveToCassandra("test", "words")
    * }}}
    *
    * With explicit column names which allows for saving only those columns specified,
    * i.e. this will not save the "other" column.
    * {{{
    *   rdd.saveToCassandra("test", "words", Seq("word", "count"))
    * }}}
    *
    * With explicit column names and batch size
    * {{{
    *   rdd.saveToCassandra("test", "words", Seq("word", "count"), Some(size)
    * }}}
    *
    * By default, writes are performed at ConsistencyLevel.ONE in order to leverage data-locality and minimize network traffic.
    * This write consistency level is controlled by the following property:
    *   - spark.cassandra.output.consistency.level: consistency level for RDD writes, string matching the ConsistencyLevel enum name.
    *
    * Scala API
    *
    * @param keyspaceName the keyspace to use
    *
    * @param tableName the table name to use
    *
    * @param columnNames Saves the data from RDD to a Cassandra table where the RDD object properties must
    *                    match Cassandra table column names. Non-selected property/column names are left
    *                    unchanged in Cassandra. All primary key columns must be selected.
    *
    * @param batchSize Saves the data from RDD to a Cassandra table in batches of given size. Use this only if
    *                  you find automatically tuned batch size doesn't result in optimal performance.
    *                  Larger batches raise memory use by temporary buffers and may incur larger GC pressure on the server.
    *                  Small batches would result in more roundtrips and worse throughput. Typically sending a few kilobytes
    *                  of data per every batch is enough to achieve good performance.
    */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: Seq[String] = Seq.empty,
                      batchSize: Option[Int] = None)(implicit rwf: RowWriterFactory[T]) {

    val writer =
      if (columnNames.isEmpty) tableWriter(keyspaceName, tableName, None, None)(rwf) else
        batchSize match {
          case None       => tableWriter(keyspaceName, tableName, Some(columnNames), None)(rwf)
          case Some(size) => tableWriter(keyspaceName, tableName, Some(columnNames), batchSize)(rwf)
        }

    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
    * Creates a [[com.datastax.spark.connector.writer.TableWriter]].
    * Internal API
    */
  private[connector] def tableWriter(keyspace: String, table: String,
                                     columns: Option[Seq[String]],
                                     batchSize: Option[Int])(implicit rwf: RowWriterFactory[T]): TableWriter[T] =
    TableWriter[T](
      connector,
      keyspaceName = keyspace,
      tableName = table,
      consistencyLevel = outputConsistencyLevel,
      columnNames = columns,
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = batchSizeInRows,
      parallelismLevel = writeParallelismLevel)
}
