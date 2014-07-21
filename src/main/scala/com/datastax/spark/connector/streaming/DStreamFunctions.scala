package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamFunctions[T: ClassTag](dstream: DStream[T]) extends Serializable {

  def saveToCassandra(keyspaceName: String, tableName: String)(implicit rwf: RowWriterFactory[T]) {
    dstream.foreachRDD(rdd => rdd.saveToCassandra(keyspaceName, tableName)(rwf))
  }

  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: Seq[String])(implicit rwf: RowWriterFactory[T]) {
    dstream.foreachRDD(rdd => rdd.saveToCassandra(keyspaceName, tableName, columnNames)(rwf))
  }

  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: Seq[String],
                      batchSize: Int)(implicit rwf: RowWriterFactory[T]) {
    dstream.foreachRDD(rdd => rdd.saveToCassandra(keyspaceName, tableName, columnNames, batchSize)(rwf))
  }
}
