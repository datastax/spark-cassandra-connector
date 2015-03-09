package com.datastax.spark.connector.japi

import java.lang.{Iterable => JIterable}

import org.apache.spark.api.java.JavaPairRDD

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.api.java.function.{Function => JFunction}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.writer.WriteConf



/**
 * A Java API wrapper over [[org.apache.spark.rdd.RDD]] to provide Spark Cassandra Connector functionality.
 *
 * To obtain an instance of this wrapper, use one of the factory methods in
 * [[com.datastax.spark.connector.japi.CassandraJavaUtil]] class.<
 */

class RDDJavaFunctions[T](val rdd: RDD[T]) extends RDDAndDStreamCommonJavaFunctions[T] {

  override def defaultConnector() = rdd.connector
  override def getConf = rdd.sparkContext.getConf

  override def saveToCassandra(keyspace: String,
                               table: String,
                               rowWriterFactory: RowWriterFactory[T],
                               columnNames: ColumnSelector,
                               conf: WriteConf,
                               connector: CassandraConnector) = {

    rdd.saveToCassandra(keyspace, table, columnNames, conf)(connector, rowWriterFactory)
  }

  override def saveToCassandra(keyspace: String,
                               table: String,
                               cluster: String,
                               rowWriterFactory: RowWriterFactory[T],
                               columnNames: ColumnSelector,
                               conf: WriteConf,
                               connector: CassandraConnector) = {

    rdd.saveToCassandra(keyspace, table, columnNames, conf, Option(cluster))(connector, rowWriterFactory)
  }

  /** Applies a function to each item, and groups consecutive items having the same value together.
    * Contrary to `groupBy`, items from the same group must be already next to each other in the
    * original collection. Works locally on each partition, so items from different
    * partitions will never be placed in the same group.*/
  def spanBy[U : ClassTag](f: JFunction[T, U]): JavaPairRDD[U, JIterable[T]] = {
    new JavaPairRDD(rdd.spanBy(f.call).mapValues(asJavaIterable))
  }
}

