package com.datastax.spark

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.{ RowWriterFactory, TableWriter }
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * The root package of Cassandra connector for Apache Spark.
 * Offers handy implicit conversions that add Cassandra-specific methods to `SparkContext` and `RDD`.
 *
 * Call [[com.datastax.spark.connector.SparkContextFunctions#cassandraTable cassandraTable]] method on the `SparkContext` object
 * to create a [[com.datastax.spark.connector.rdd.CassandraRDD CassandraRDD]] exposing Cassandra tables as Spark RDDs.
 *
 * Call [[com.datastax.spark.connector.RDDFunctions#saveToCassandra saveToCassandra]]
 * method on any `RDD` to save distributed collection to a Cassandra table.
 *
 * Example:
 * {{{
 *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
 *   CREATE TABLE test.words (word text PRIMARY KEY, count int);
 *   INSERT INTO test.words(word, count) VALUES ("and", 50);
 * }}}
 *
 * {{{
 *   import com.datastax.spark.connector._
 *
 *   val sparkMasterHost = "127.0.0.1"
 *   val cassandraHost = "127.0.0.1"
 *   val keyspace = "test"
 *   val table = "words"
 *
 *   // Tell Spark the address of one Cassandra node:
 *   val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
 *
 *   // Connect to the Spark cluster:
 *   val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "example", conf)
 *
 *   // Read the table and print its contents:
 *   val rdd = sc.cassandraTable(keyspace, table)
 *   rdd.toArray().foreach(println)
 *
 *   // Write two rows to the table:
 *   val col = sc.parallelize(Seq(("of", 1200), ("the", "863")))
 *   col.saveToCassandra(keyspace, table)
 *
 *   sc.stop()
 * }}}
 */
package object connector {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)

  implicit def toRDDFunctions[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] =
    new RDDFunctions[T](rdd)

}
