package com.datastax.driver.spark

import com.datastax.driver.spark
import com.datastax.driver.spark.rdd.CassandraRDD
import com.datastax.driver.spark.rdd.reader.RowReaderFactory
import org.apache.spark.SparkContext
import java.io.{Serializable => JavaSerializable}
import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `SparkContext` */
class SparkContextFunctions(sc: SparkContext) {

  /** Returns a view of a Cassandra table as `CassandraRDD`.
    * This method is made available on `SparkContext` by importing `com.datastax.driver.spark._`
    *
    * Depending on the type parameter passed to `cassandraTable`, every row is converted to one of the following:
    *   - an [[rdd.reader.CassandraRow]] object (default, if no type given)
    *   - a tuple containing column values in the same order as columns selected by [[spark.rdd.CassandraRDD#select CassandraRDD#select]]
    *   - object of a user defined class, populated by appropriate [[spark.mapper.ColumnMapper ColumnMapper]]
    *
    * Example:
    * {{{
    *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
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
  def cassandraTable[T <: JavaSerializable : ClassTag : RowReaderFactory](keyspace: String, table: String): CassandraRDD[T] =
    new CassandraRDD[T](sc, keyspace, table)
}
