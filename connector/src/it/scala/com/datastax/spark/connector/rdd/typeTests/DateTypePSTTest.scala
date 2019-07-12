package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.datastax.spark.connector.cluster.PSTCluster
import org.scalatest.Ignore

// FIXME: remove @Ignore after driver upgrade
@Ignore
class DateTimeTypePSTTest extends DateTimeTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}

@Ignore
class DateTypePSTTest extends DateTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}

@Ignore
class SqlDateTypePSTTest extends SqlDateTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}
