package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.datastax.spark.connector.cluster.PSTCluster

class DateTimeTypePSTTest extends DateTimeTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}

class DateTypePSTTest extends DateTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}

class SqlDateTypePSTTest extends SqlDateTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}
