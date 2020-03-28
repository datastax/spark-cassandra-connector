package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.TableRef
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

class CassandraStreamingSinkRelation(
  tableRef: TableRef,
  sqlContext: SQLContext,
  options: CassandraSourceOptions,
  outputMode: OutputMode) extends Sink {

  /*
    * Currently all we will do is execute a standard DataSource append. In the future we may consider
    * if some methods like "Complete" will end up causing issues since all of the timestamps for a
    * row will be identical leading to "Greatest Value Wins" with a LWW Conflict. At the moment,
    * I don't think this is crucial to figure out as I think it's unlikely that a dataframe will
    * contain multiple updates for the same key (and if it does we probably cannot handle this)
    */
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    CassandraSourceRelation(tableRef, sqlContext, options, None).insert(data, overwrite = false)
  }
}

object CassandraStreamingSinkRelation {

  def apply(
    tableRef: TableRef,
    sqlContext: SQLContext,
    options: CassandraSourceOptions,
    outputMode: OutputMode): CassandraStreamingSinkRelation = {

    new CassandraStreamingSinkRelation(tableRef, sqlContext, options, outputMode)
  }
}
