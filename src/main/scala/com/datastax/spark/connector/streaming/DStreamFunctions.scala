package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{Fields, TableWriter, RowWriterFactory}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends Serializable {

  /** Performs [[com.datastax.spark.connector.RDDFunctions]] for each produced RDD.
    *
    * @param keyspaceName The keyspace to use
    *
    * @param tableName The table name to use
    *
    * @param columnNames The list of columns to save data to.
    *                    If specified, uses only the unique column names, and you must select at least all primary key
    *                    columns. All other fields are discarded. Non-selected property/column names are left unchanged.
    *                    If not specified, will save data to all columns in the Cassandra table.
    *                    Defaults to all columns: `TableWriter.AllColumns`.
    *
    * @param batchSize The batch size. By default, if the batch size is unspecified, the right amount
    *                  is calculated automatically according the average row size. Specify explicit value
    *                  here only if you find automatically tuned batch size doesn't result in optimal performance.
    *                  Larger batches raise memory use by temporary buffers and may incur larger GC pressure on the server.
    *                  Small batches would result in more roundtrips and worse throughput. Typically sending a few kilobytes
    *                  of data per every batch is enough to achieve good performance. Defaults to `None`.
    */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: Seq[String] = Fields.ALL,
                      batchSize: Option[Int] = None)(implicit rwf: RowWriterFactory[T]) {

    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName, columnNames, batchSize)(rwf))

  }
}
