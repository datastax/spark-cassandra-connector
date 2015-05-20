package com.datastax.spark.connector.sql

class CassandraPrunedScanSpec extends CassandraDataSourceSpec {
  override def pushDown = false
}