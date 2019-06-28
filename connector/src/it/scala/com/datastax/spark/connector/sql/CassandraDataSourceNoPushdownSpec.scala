package com.datastax.spark.connector.sql

class CassandraDataSourceNoPushdownSpec extends CassandraDataSourceSpec {
  override def pushDown: Boolean = false
}
