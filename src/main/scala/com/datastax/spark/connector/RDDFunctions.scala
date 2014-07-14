package com.datastax.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{RowWriterFactory, TableWriter}
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {

  private lazy val batchSizeInRowsStr = rdd.sparkContext.getConf.get(
    "spark.cassandra.output.batch.size.rows", "auto")

  private lazy val batchSizeInBytes = rdd.sparkContext.getConf.getInt(
    "spark.cassandra.output.batch.size.bytes", TableWriter.DefaultBatchSizeInBytes)

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
    * {{{
    *   case class WordCount(word: String, count: Int, other: String)
    *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
    *   rdd.saveToCassandra("test", "words")
    * }}} */
  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T]) {
    val writer = TableWriter[T](
      connector, keyspaceName, tableName,
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = batchSizeInRows,
      parallelismLevel = writeParallelismLevel)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /** Saves the data from `RDD` to a Cassandra table.
    * The RDD object properties must match Cassandra table column names.
    * Non-selected property/column names are left unchanged in Cassandra.
    * All primary key columns must be selected.
    *
    * Example:
    * {{{
    *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    *   CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
    * }}}
    * {{{
    *   case class WordCount(word: String, count: Int, other: String)
    *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
    *   rdd.saveToCassandra("test", "words", Seq("word", "count"))   // will not save the "other" column
    * }}} */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: Seq[String])(implicit rwf: RowWriterFactory[T]) {

    val writer = TableWriter[T](
      connector, keyspaceName, tableName, columnNames = Some(columnNames),
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = batchSizeInRows,
      parallelismLevel = writeParallelismLevel)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /** Saves the data from RDD to a Cassandra table in batches of given size.
    * Use this overload only if you find automatically tuned batch size doesn't result in optimal performance.
    *
    * Larger batches raise memory use by temporary buffers and may incur
    * larger GC pressure on the server. Small batches would result in more roundtrips
    * and worse throughput. Typically sending a few kilobytes of data per every batch
    * is enough to achieve good performance. */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: Seq[String],
                      batchSize: Int)(implicit rwf: RowWriterFactory[T]) {

    val writer = TableWriter[T](
      connector, keyspaceName, tableName, columnNames = Some(columnNames),
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = Some(batchSize),
      parallelismLevel = writeParallelismLevel)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }
}
