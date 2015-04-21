package com.datastax.spark.connector.sql.source

import org.apache.spark.sql.cassandra.DefaultSource._

class CatalystScanSpec extends CassandraDataSourceSpec {
  override val scanType = CassandraDataSourceCatalystScanTypeName
}

class CatalystScanFilterPushdownSpec extends CassandraDataSourceFilterPushdownSpec {
  override val scanType = CassandraDataSourceCatalystScanTypeName
}

class CatalystScanClusterLevelSpec extends CassandraDataSourceClusterLevelSpec {
  override val scanType = CassandraDataSourceCatalystScanTypeName
}