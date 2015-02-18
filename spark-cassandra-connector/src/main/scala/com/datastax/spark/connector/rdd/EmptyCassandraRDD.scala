package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.{TaskContext, Partition, SparkContext}

import scala.reflect.ClassTag

/** EmptyCassandraRDD is just a fake subclass of CassandraRDD which does not perform any validation
  * and does not return any rows. */
class EmptyCassandraRDD[R](@transient sc: SparkContext,
                           connector: CassandraConnector,
                           keyspaceName: String,
                           tableName: String)
                          (implicit ct: ClassTag[R], @transient rtf: RowReaderFactory[R])

  extends CassandraRDD[R](sc, connector, keyspaceName, tableName) {

  override def getPartitions: Array[Partition] = Array.empty

}
