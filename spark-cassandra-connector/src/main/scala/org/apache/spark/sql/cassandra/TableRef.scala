package org.apache.spark.sql.cassandra

import org.apache.spark.sql.SQLContext


/** Store table name, keyspace name and option cluster name, keyspace is equivalent to database */
case class TableRef(table: String, keyspace: String, cluster: Option[String] = None)

object TableRef {
  def apply(table: String, sqlContext: SQLContext) {
    TableRef(table, sqlContext.getKeyspace, Option(sqlContext.getCluster))
  }
}

