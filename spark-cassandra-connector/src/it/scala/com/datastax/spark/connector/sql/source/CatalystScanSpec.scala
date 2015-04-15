package com.datastax.spark.connector.sql.source

import org.apache.spark.sql.cassandra.DefaultSource._

class CatalystScanSpec extends CassandraDataSourceSpec {
  override def setScanType() = {
    scanType = CassandraDataSourceCatalystScanTypeName
  }
}

class CatalystScanFilterPushdownSpec extends CassandraDataSourceFilterPushdownSpec {
  override def setScanType() = {
    scanType = CassandraDataSourceCatalystScanTypeName
  }
}

class CatalystScanClusterLevelSpec extends CassandraDataSourceClusterLevelSpec {
  override def setScanType() = {
    scanType = CassandraDataSourceCatalystScanTypeName
  }
}