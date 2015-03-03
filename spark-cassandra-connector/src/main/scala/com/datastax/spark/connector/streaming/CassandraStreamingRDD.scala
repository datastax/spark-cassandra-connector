package com.datastax.spark.connector.streaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{ColumnSelector, AllColumns}

import scala.reflect.ClassTag
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.rdd.{ReadConf, CassandraRDD, CqlWhereClause}
import com.datastax.spark.connector.rdd.reader._

/** RDD representing a Cassandra table for Spark Streaming.
  * @see [[com.datastax.spark.connector.rdd.CassandraRDD]] */
class CassandraStreamingRDD[R] private[connector] (
    sctx: StreamingContext,
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    columns: ColumnSelector = AllColumns,
    where: CqlWhereClause = CqlWhereClause.empty,
    empty: Boolean = false,
    limit: Option[Long] = None,
    readConf: ReadConf = ReadConf())(
  implicit
    ct : ClassTag[R],
    @transient rrf: RowReaderFactory[R])
  extends CassandraRDD[R](sctx.sparkContext, connector, keyspace, table, columns, where, empty, limit, readConf)
