package com.datastax.spark.connector.streaming

import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag
import com.datastax.spark.connector.rdd.{CassandraRDD, CqlWhereClause, AllColumns, ColumnSelector}
import com.datastax.spark.connector.rdd.reader._

/** RDD representing a Cassandra table for Spark Streaming.
  * @see [[CassandraRDD]]
  */
class CassandraStreamingRDD[R] private[connector] (
                                                    sctx: StreamingContext,
                                                    keyspace: String,
                                                    table: String,
                                                    columns: ColumnSelector = AllColumns,
                                                    where: CqlWhereClause = CqlWhereClause.empty)(
                                                    implicit
                                                    ct : ClassTag[R], @transient rtf: RowReaderFactory[R])
  extends CassandraRDD[R](sctx.sparkContext, keyspace, table, columns, where)
