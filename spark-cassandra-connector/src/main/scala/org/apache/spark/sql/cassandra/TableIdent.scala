package org.apache.spark.sql.cassandra


/** Store table identity including table name, keyspace name and option cluster name */
case class TableIdent(table: String, keyspace: String, cluster: Option[String] = None)

