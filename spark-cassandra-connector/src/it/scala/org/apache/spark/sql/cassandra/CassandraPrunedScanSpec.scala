package org.apache.spark.sql.cassandra

class CassandraPrunedScanSpec extends CassandraDataSourceSpec {
  override def pushDown = false
}