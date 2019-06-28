package com.datastax.spark.connector.sql

import java.util.TimeZone

import org.scalatest.FlatSpec

/**
  * This should be executed in separate JVM, as Catalyst caches default time zone
  */
class CassandraDataFrameDatePSTSpec  extends FlatSpec with CassandraDataFrameDateBehaviors {

  val pacificTimeZone = TimeZone.getTimeZone("PST")

  "A DataFrame in PST timezone" should behave like dataFrame(pacificTimeZone)
}
