package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.datastax.spark.connector.cluster.CETCluster

class DateTypeCETTest extends DateTypeTest(TimeZone.getTimeZone("CET")) with CETCluster {
}

class SqlDateTypeCETTest extends SqlDateTypeTest(TimeZone.getTimeZone("CET")) with CETCluster {
}
