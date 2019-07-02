package org.apache.spark.sql.cassandra


/** Store table name, keyspace name and option cluster name, keyspace is equivalent to database */
case class TableRef(table: String, keyspace: String, cluster: Option[String] = None)

