package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.datastax.spark.connector.cluster.{CETCluster, CSTCluster}

class DateTypeCSTTest extends DateTypeTest(TimeZone.getTimeZone("CST")) with CSTCluster {
}

class SqlDateTypeCSTTest extends SqlDateTypeTest(TimeZone.getTimeZone("CST")) with CSTCluster {
}
