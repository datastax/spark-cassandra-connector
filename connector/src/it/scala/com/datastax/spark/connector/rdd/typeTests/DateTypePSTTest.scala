package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

/**
  * Following tests are executed in separate JVM
  */
class DateTimeTypePSTTest extends DateTimeTypeTest(TimeZone.getTimeZone("PST")) {
}

class DateTypePSTTest extends DateTypeTest(TimeZone.getTimeZone("PST")) {
}

class SqlDateTypePSTTest extends SqlDateTypeTest(TimeZone.getTimeZone("PST")) {
}
