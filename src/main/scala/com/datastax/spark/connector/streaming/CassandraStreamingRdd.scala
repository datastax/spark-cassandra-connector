/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector.streaming

import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
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
  extends CassandraRDD[R](sctx.sparkContext, keyspace, table, columns, where) {

  override def compute(split: Partition, context: TaskContext): Iterator[R] = super.compute(split, context)

  override def getPartitions: Array[Partition] = super.getPartitions
}
