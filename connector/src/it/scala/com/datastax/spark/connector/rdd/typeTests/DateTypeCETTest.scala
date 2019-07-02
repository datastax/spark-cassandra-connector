package com.datastax.spark.connector.rdd.typeTests

import java.util.TimeZone

/**
  * Following tests are executed in separate JVM
  */
class DateTimeTypeCETTest extends DateTimeTypeTest(TimeZone.getTimeZone("CET")) {
}

class DateTypeCETTest extends DateTypeTest(TimeZone.getTimeZone("CET")) {
}

class SqlDateTypeCETTest extends SqlDateTypeTest(TimeZone.getTimeZone("CET")) {
}
