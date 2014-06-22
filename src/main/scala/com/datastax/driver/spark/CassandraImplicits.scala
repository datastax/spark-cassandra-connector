package com.datastax.driver.spark

import com.datastax.driver.spark.connector.CassandraConnector
import com.datastax.driver.spark.mapper.{RowTransformerFactory, ColumnMapper}
import com.datastax.driver.spark.rdd.CassandraRDD
import com.datastax.driver.spark.writer.CassandraWriter
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object CassandraImplicits {

  implicit class SparkContextFunctions(sc: SparkContext) {

    /** Returns a view of Cassandra table as Spark RDD */
    def cassandraTable[T <: Serializable : ClassTag : RowTransformerFactory](keyspace: String, table: String): CassandraRDD[T] =
      new CassandraRDD[T](sc, keyspace, table)
  }

  implicit class WriteFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {

    private lazy val batchSizeInRowsStr = rdd.sparkContext.getConf.get(
      "cassandra.output.batch.size.rows", "auto")

    private lazy val batchSizeInBytes = rdd.sparkContext.getConf.getInt(
      "cassandra.output.batch.size.bytes", CassandraWriter.DefaultBatchSizeInBytes)

    private lazy val batchSizeInRows = {
      val Number = "([0-9]+)".r
      batchSizeInRowsStr match {
        case "auto" => None
        case Number(x) => Some(x.toInt)
        case other =>
          throw new ConfigurationException(
            s"Invalid value of cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
      }
    }

    private lazy val writeParallelismLevel = rdd.sparkContext.getConf.getInt(
      "cassandra.output.concurrent.writes", CassandraWriter.DefaultParallelismLevel)

    private lazy val connector = CassandraConnector(rdd.sparkContext.getConf)

    /** Saves the data from RDD to a Cassandra table.
      * Saves all properties that have corresponding Cassandra columns.
      * The underlying RDD class must provide data for all columns.
      *
      * Example:
      * {{{
      * CQL:
      *   CREATE TABLE words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
      * Scala:
      *   case class WordCount(word: String, count: Int, other: String)
      *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
      *   rdd.saveToCassandra("ks", "words")
      * }}} */
    def saveToCassandra(keyspaceName: String, tableName: String)(implicit ccm: ColumnMapper[T]) {
      val writer = CassandraWriter[T](
        connector, keyspaceName, tableName,
        batchSizeInBytes = batchSizeInBytes,
        batchSizeInRows = batchSizeInRows,
        parallelismLevel = writeParallelismLevel)
      rdd.sparkContext.runJob(rdd, writer.write _)
    }

    /** Saves the data from RDD to a Cassandra table.
      * The RDD object properties must match Cassandra table column names.
      * Non-selected property/column names are left unchanged in Cassandra.
      * All primary key columns must be selected.
      * Example:
      * {{{
      * CQL:
      *   CREATE TABLE words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
      * Scala:
      *   case class WordCount(word: String, count: Int, other: String)
      *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
      *   rdd.saveToCassandra("ks", "words", Seq("word", "count"))   // will not save the "other" column
      * }}} */
    def saveToCassandra(keyspaceName: String,
                        tableName: String,
                        columnNames: Seq[String])(implicit ccm: ColumnMapper[T]) {

      val writer = CassandraWriter[T](
        connector, keyspaceName, tableName, columnNames = Some(columnNames),
        batchSizeInBytes = batchSizeInBytes,
        batchSizeInRows = batchSizeInRows,
        parallelismLevel = writeParallelismLevel)
      rdd.sparkContext.runJob(rdd, writer.write _)
    }

    /** Saves the data from RDD to a Cassandra table.
      * Additionally allows to specify batch size in rows.
      * Use this overload only if you find automatically
      * tuned batch size doesn't result in optimal performance. */
    def saveToCassandra(keyspaceName: String,
                        tableName: String,
                        columnNames: Seq[String],
                        batchSize: Int)(implicit ccm: ColumnMapper[T]) {

      val writer = CassandraWriter[T](
        connector, keyspaceName, tableName, columnNames = Some(columnNames),
        batchSizeInBytes = batchSizeInBytes,
        batchSizeInRows = Some(batchSize),
        parallelismLevel = writeParallelismLevel)
      rdd.sparkContext.runJob(rdd, writer.write _)
    }
  }
}
