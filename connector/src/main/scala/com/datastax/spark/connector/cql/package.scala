package com.datastax.spark.connector

import com.datastax.driver.core.Row


/** Contains a [[cql.CassandraConnector]] object which is used to connect
  * to a Cassandra cluster and to send CQL statements to it. `CassandraConnector`
  * provides a Scala-idiomatic way of working with `Cluster` and `Session` object
  * and takes care of connection pooling and proper resource disposal.*/
package object cql {

  def getRowBinarySize(row: Row): Int = {
    var size = 0
    for (i <- 0 until row.getColumnDefinitions.size() if !row.isNull(i))
      size += row.getBytesUnsafe(i).remaining()
    size
  }

}
