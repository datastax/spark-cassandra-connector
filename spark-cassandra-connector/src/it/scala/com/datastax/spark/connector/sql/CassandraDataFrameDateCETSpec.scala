package com.datastax.spark.connector.sql

import java.util.TimeZone

import org.scalatest.FlatSpec

/**
  * This should be executed in separate JVM, as Catalyst caches default time zone
  */
class CassandraDataFrameDateCETSpec  extends FlatSpec with CassandraDataFrameDateBehaviors {

  val centralEuropeanTimeZone = TimeZone.getTimeZone("CET")

  "A DataFrame in CET timezone" should behave like dataFrame(centralEuropeanTimeZone)
}
