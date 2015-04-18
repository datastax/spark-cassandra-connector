package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.{TableWriter, WriteConf, RowWriterFactory, WritableToCassandra}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamFunctions[T](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  override def sparkContext: SparkContext = dstream.context.sparkContext

  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra(
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector = AllColumns,
    writeConf: WriteConf = WriteConf.fromSparkConf(conf))(
  implicit
    connector: CassandraConnector = CassandraConnector(conf),
    rwf: RowWriterFactory[T]): Unit = {

    val writer = TableWriter(connector, keyspaceName, tableName, columnNames, writeConf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  /**
   * Transforms RDDs with [[com.datastax.spark.connector.RDDFunctions.repartitionByCassandraReplica]]
   * for each produced RDD.
   */
  def repartitionByCassandraReplica(
    keyspaceName: String,
    tableName: String,
    partitionsPerHost: Int = 10,
    partitionKeyMapper: ColumnSelector = PartitionKeyColumns)
    (
  implicit
    connector: CassandraConnector = CassandraConnector(conf),
    currentType: ClassTag[T],
    rwf: RowWriterFactory[T]): DStream[T] = {

    dstream.transform(rdd =>
      rdd.repartitionByCassandraReplica(keyspaceName, tableName, partitionsPerHost, partitionKeyMapper))
  }

  /**
   * Transforms RDDs with [[com.datastax.spark.connector.RDDFunctions.joinWithCassandraTable]]
   * for each produced RDD
   */
  def joinWithCassandraTable[R](
    keyspaceName: String, tableName: String,
    selectedColumns: ColumnSelector = AllColumns,
    joinColumns: ColumnSelector = PartitionKeyColumns)(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
    newType: ClassTag[R],
    rrf: RowReaderFactory[R],
    ev: ValidRDDType[R],
    currentType: ClassTag[T],
    rwf: RowWriterFactory[T]): DStream[(T,R)] = {

    dstream.transform(rdd =>
      rdd.joinWithCassandraTable[R](keyspaceName, tableName, selectedColumns, joinColumns))
  }
}
