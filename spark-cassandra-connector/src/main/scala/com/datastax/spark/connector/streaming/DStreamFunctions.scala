package com.datastax.spark.connector.streaming

import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{WritableToCassandra, RowWriterFactory}
import com.datastax.spark.connector.rdd._

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  /** Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
    * Uses all column names. */
  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T]): Unit =
    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName)(rwf))

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names.
   */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: SomeColumns)(implicit rwf: RowWriterFactory[T]): Unit =
    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName, columnNames)(rwf))


  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: SomeColumns, batchSize: Int)(implicit rwf: RowWriterFactory[T]): Unit =
    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName, columnNames, batchSize)(rwf))
}
