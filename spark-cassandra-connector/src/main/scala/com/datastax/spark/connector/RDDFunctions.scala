package com.datastax.spark.connector

import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{ WritableToCassandra, Fields, RowWriterFactory, TableWriter }
import com.datastax.driver.core.ConsistencyLevel

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T: ClassTag](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  private lazy val batchSizeInRowsStr = rdd.sparkContext.getConf.get(
    "spark.cassandra.output.batch.size.rows", "auto")

  private lazy val batchSizeInBytes = rdd.sparkContext.getConf.getInt(
    "spark.cassandra.output.batch.size.bytes", TableWriter.DefaultBatchSizeInBytes)

  private lazy val outputConsistencyLevel = ConsistencyLevel.valueOf(
    rdd.sparkContext.getConf.get("spark.cassandra.output.consistency.level", ConsistencyLevel.LOCAL_ONE.name))

  private lazy val batchSizeInRows = {
    val Number = "([0-9]+)".r
    batchSizeInRowsStr match {
      case "auto"    => None
      case Number(x) => Some(x.toInt)
      case other =>
        throw new ConfigurationException(
          s"Invalid value of spark.cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
    }
  }

  private lazy val writeParallelismLevel = rdd.sparkContext.getConf.getInt(
    "spark.cassandra.output.concurrent.writes", TableWriter.DefaultParallelismLevel)

  private lazy val connector = CassandraConnector(rdd.sparkContext.getConf)

  /** Saves the data from `RDD` to a Cassandra table. */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: Seq[String] = Fields.ALL,
                      batchSize: Option[Int] = None)(implicit rwf: RowWriterFactory[T]) {

    val writer = batchSize match {
      case None       => tableWriter(keyspaceName, tableName, columnNames, None)(rwf)
      case Some(size) => tableWriter(keyspaceName, tableName, columnNames, batchSize)(rwf)
    }

    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /**
   * Creates a [[com.datastax.spark.connector.writer.TableWriter]].
   * Internal API
   */
  private[connector] def tableWriter(keyspace: String, table: String,
                                     columns: Seq[String],
                                     batchSize: Option[Int])(implicit rwf: RowWriterFactory[T]): TableWriter[T] =
    TableWriter[T](
      connector,
      keyspaceName = keyspace,
      tableName = table,
      consistencyLevel = outputConsistencyLevel,
      columnNames = columns,
      batchSizeInBytes = batchSizeInBytes,
      batchSizeInRows = batchSizeInRows,
      parallelismLevel = writeParallelismLevel)
}
