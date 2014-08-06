package com.datastax.spark.connector

import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector.rdd.{AllColumns, SomeColumns, ColumnSelector}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer._
import com.datastax.driver.core.ConsistencyLevel

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T : ClassTag](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

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

  /**
   * Saves the data from `RDD` to a Cassandra table.
   * {{{
   *   rdd.saveToCassandra(AllColumns("test", "words"))
   * }}}
   * @see [[WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, AllColumns, None)(rwf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String, tableName: String, columns: SomeColumns)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, columns, None)(rwf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names with an additional batch size.
   * @see [[WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String, tableName: String, columns: SomeColumns, batchSize: Int)(implicit rwf: RowWriterFactory[T]): Unit = {
    val writer = tableWriter(keyspaceName, tableName, columns, Some(batchSize))(rwf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Internal API.
    * Creates a [[com.datastax.spark.connector.writer.TableWriter]].
    */
  private[connector] def tableWriter(keyspaceName: String, tableName: String,
                                     columns: ColumnSelector, batchSize: Option[Int])(implicit rwf: RowWriterFactory[T]): TableWriter[T] =

    TableWriter[T](
      connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      consistencyLevel = outputConsistencyLevel,
      columnNames = columns,
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = batchSize,
      parallelismLevel = writeParallelismLevel)

}
