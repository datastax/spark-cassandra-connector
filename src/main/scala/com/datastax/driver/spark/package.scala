package com.datastax.driver

import com.datastax.driver.spark.connector.CassandraConnector
import com.datastax.driver.spark.mapper.{ColumnMapper, RowTransformerFactory}
import com.datastax.driver.spark.rdd.CassandraRDD
import com.datastax.driver.spark.writer.CassandraWriter
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * The root package of Cassandra driver for Apache Spark.
 * Offers handy implicit conversions that add Cassandra-specific methods to `SparkContext` and `RDD`.
 *
 * Call `cassandraTable` method on the `SparkContext` object
 * to create a [[com.datastax.driver.spark.rdd.CassandraRDD]] exposing Cassandra tables as Spark RDDs.
 *
 * Call `saveToCassandra` method on any `RDD` to save distributed collection to a Cassandra table.
 *
 * Example program:
 * {{{
 *   import com.datastax.driver.spark._
 *
 *   val sparkMasterHost = "127.0.0.1"
 *   val cassandraHost = "127.0.0.1"
 *   val keyspace = "test"
 *   val table = "kv"
 *
 *   // Tell Spark the address of one Cassandra node:
 *   val conf = new SparkConf(true).set("cassandra.connection.host", cassandraHost)
 *
 *   // Connect to the Spark cluster:
 *   val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "demo-program", conf)
 *
 *   // Read table test.kv and print its contents:
 *   val rdd = sc.cassandraTable("test", "kv").select("key", "value")
 *   rdd.toArray().foreach(println)
 *
 *   // Write two rows to the test.kv table:
 *   val col = sc.parallelize(Seq((1, "value 1"), (2, "value 2")))
 *   col.saveToCassandra("test", "kv", Seq("key", "value"))
 *
 *   sc.stop()
 * }}}
 */
package object spark {

  /** Provides Cassandra-specific methods on `SparkContext` */
  implicit class SparkContextFunctions(sc: SparkContext) {

    /** Returns a view of a Cassandra table as `CassandraRDD`.
      * This method is made available on `SparkContext` by importing `com.datastax.driver.spark._`
      *
      * Depending on the type parameter passed to `cassandraTable`, every row is converted to one of the following:
      *   - a [[com.datastax.driver.spark.rdd.CassandraRow]] object (default, if no type given)
      *   - a tuple containing column values in the same order as columns selected by [[com.datastax.driver.spark.rdd.CassandraRDD#select]]
      *   - object of a user defined class, populated by appropriate [[com.datastax.driver.spark.mapper.ColumnMapper]]
      *
      * Example:
      * {{{
      *   CREATE TABLE test.words (word text PRIMARY KEY, count int);
      *   INSERT INTO test.words (word, count) VALUES ('foo', 20);
      *   INSERT INTO test.words (word, count) VALUES ('bar', 20);
      *   ...
      * }}}
      * {{{
      *   // Obtaining RDD of CassandraRow objects:
      *   val rdd1 = sc.cassandraTable("test", "words")
      *   rdd1.first.getString("word")  // foo
      *   rdd1.first.getInt("count")    // 20
      *
      *   // Obtaining RDD of tuples:
      *   val rdd2 = sc.cassandraTable[(String, Int)]("test", "words").select("word", "count")
      *   rdd2.first._1  // foo
      *   rdd2.first._2  // 20
      *
      *   // Obtaining RDD of user defined objects:
      *   case class WordCount(word: String, count: Int)
      *   val rdd3 = sc.cassandraTable[WordCount]("test", "words")
      *   rdd3.first.word  // foo
      *   rdd3.first.count // 20
      * }}}*/
    def cassandraTable[T <: Serializable : ClassTag : RowTransformerFactory](keyspace: String, table: String): CassandraRDD[T] =
      new CassandraRDD[T](sc, keyspace, table)
  }

  /** Provides Cassandra-specific methods on `RDD` */
  implicit class RDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {

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

    /** Saves the data from `RDD` to a Cassandra table.
      * Saves all properties that have corresponding Cassandra columns.
      * The underlying RDD class must provide data for all columns.
      *
      * Example:
      * {{{
      *   CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
      * }}}
      * {{{
      *   case class WordCount(word: String, count: Int, other: String)
      *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
      *   rdd.saveToCassandra("test", "words")
      * }}} */
    def saveToCassandra(keyspaceName: String, tableName: String)(implicit ccm: ColumnMapper[T]) {
      val writer = CassandraWriter[T](
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
      *   CREATE TABLE test.words(word VARCHAR PRIMARY KEY, count INT, other VARCHAR);
      * }}}
      * {{{
      *   case class WordCount(word: String, count: Int, other: String)
      *   val rdd = sc.parallelize(Seq(WordCount("foo", 5, "bar")))
      *   rdd.saveToCassandra("test", "words", Seq("word", "count"))   // will not save the "other" column
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
