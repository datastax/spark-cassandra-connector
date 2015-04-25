package com.datastax.spark.connector.sql.source


class CassandraPrunedScanSpec extends CassandraDataSourceSpec {
    override def pushDown = false
}

class CassandraPrunedScanClusterLevelSpec extends CassandraDataSourceClusterLevelSpec {
  override def pushDown = false
}