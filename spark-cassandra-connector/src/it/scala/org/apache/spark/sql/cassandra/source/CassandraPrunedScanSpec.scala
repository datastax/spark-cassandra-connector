package org.apache.spark.sql.cassandra.source


class CassandraPrunedScanSpec extends CassandraDataSourceSpec {
    override def pushDown = false
}

class CassandraPrunedScanClusterLevelSpec extends CassandraDataSourceClusterLevelSpec {
  override def pushDown = false
}