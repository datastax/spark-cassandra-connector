package com.datastax.spark.connector.sql

import java.util.TimeZone

import com.datastax.spark.connector.cluster.PSTCluster
import org.scalatest.FlatSpec

/**
  * This should be executed in separate JVM, as Catalyst caches default time zone
  */
class CassandraDataFrameDatePSTSpec extends FlatSpec with CassandraDataFrameDateBehaviors with PSTCluster {

  val pacificTimeZone = TimeZone.getTimeZone("PST")

  "A DataFrame in PST timezone" should behave like dataFrame(pacificTimeZone)
}
