package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.datastax.spark.connector.cluster.CETCluster
import org.scalatest.Ignore

// FIXME: remove @Ignore after driver upgrade
@Ignore
class DateTypeCETTest extends DateTypeTest(TimeZone.getTimeZone("CET")) with CETCluster {
}

@Ignore
class SqlDateTypeCETTest extends SqlDateTypeTest(TimeZone.getTimeZone("CET")) with CETCluster {
}
