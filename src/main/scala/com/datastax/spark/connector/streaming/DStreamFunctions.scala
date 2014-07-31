package com.datastax.spark.connector.streaming

import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{WritableToCassandra, Fields, RowWriterFactory}

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  /** Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD. */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: Seq[String] = Fields.ALL,
                      batchSize: Option[Int] = None)(implicit rwf: RowWriterFactory[T]) {

    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName, columnNames, batchSize)(rwf))

  }
}
