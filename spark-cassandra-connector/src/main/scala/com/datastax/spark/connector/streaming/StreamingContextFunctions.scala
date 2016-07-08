package com.datastax.spark.connector.streaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.{ReadConf, ValidRDDType}
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.SparkContextFunctions
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

/** Provides Cassandra-specific methods on `org.apache.spark.streaming.StreamingContext`.
  * @param ssc the Spark Streaming context
  */
class StreamingContextFunctions (ssc: StreamingContext) extends SparkContextFunctions(ssc.sparkContext) {
  import scala.reflect.ClassTag

  override def cassandraTable[T](keyspace: String, table: String)(
    implicit
      connector: CassandraConnector = CassandraConnector(ssc.sparkContext.getConf),
      readConf: ReadConf = ReadConf.fromSparkConf(sc.getConf),
      ct: ClassTag[T],
      rrf: RowReaderFactory[T],
      ev: ValidRDDType[T]): CassandraStreamingRDD[T] = {

    new CassandraStreamingRDD[T](ssc, connector, keyspace, table, readConf = readConf)
  }
}
