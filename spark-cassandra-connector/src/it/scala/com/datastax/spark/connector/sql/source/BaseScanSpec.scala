package com.datastax.spark.connector.sql.source

import org.apache.spark.sql.cassandra.DefaultSource._

class BaseScanSpec extends CassandraDataSourceSpec {
  override def setScanType() = {
    scanType = CassandraDataSourceBaseScanTypeName
  }
}

class BaseScanClusterLevelSpec extends CassandraDataSourceClusterLevelSpec {
  override def setScanType() = {
    scanType = CassandraDataSourceBaseScanTypeName
  }
}