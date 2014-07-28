package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends Serializable {

  /** Performs [[com.datastax.spark.connector.RDDFunctions]] for each produced RDD. */
  def saveToCassandra(keyspaceName: String, tableName: String,
                      columnNames: Seq[String] = Seq.empty,
                      batchSize: Option[Int] = None)(implicit rwf: RowWriterFactory[T]) {

    dstream.foreachRDD(_.saveToCassandra(keyspaceName, tableName, columnNames, batchSize)(rwf))

  }
}
