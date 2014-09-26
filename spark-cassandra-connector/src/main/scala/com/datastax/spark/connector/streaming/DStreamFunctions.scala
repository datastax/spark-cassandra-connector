package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{RowWriterFactory, WritableToCassandra}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  override def sparkContext: SparkContext = dstream.context.sparkContext

  /** Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
    * Uses all column names. */
  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, AllColumns, None)(rwf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names.
   */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: SomeColumns)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, columnNames, None)(rwf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: SomeColumns, batchSize: Int)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, columnNames, Some(batchSize))(rwf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }
}
