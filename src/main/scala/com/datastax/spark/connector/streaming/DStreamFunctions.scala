package com.datastax.spark.connector.streaming

import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{Fields, RowWriterFactory}

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  /** Performs [[com.datastax.spark.connector.WritableToCassandra.saveToCassandra()]] for each produced RDD. */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: Seq[String] = Fields.ALL,
                      batchSize: Option[Int] = None)(implicit rwf: RowWriterFactory[T]) {

    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName, columnNames, batchSize)(rwf))

  }
}
