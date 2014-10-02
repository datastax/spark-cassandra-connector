package com.datastax.spark.connector

import com.datastax.spark.connector.writer._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T : ClassTag](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  override val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Saves the data from `RDD` to a Cassandra table.
   * {{{
   *   rdd.saveToCassandra(AllColumns("test", "words"))
   * }}}
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, AllColumns, None)(rwf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String, tableName: String, columns: SomeColumns)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, columns, None)(rwf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names with an additional batch size.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String, tableName: String, columns: SomeColumns, batchSize: Int)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, columns, Some(batchSize))(rwf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

}
