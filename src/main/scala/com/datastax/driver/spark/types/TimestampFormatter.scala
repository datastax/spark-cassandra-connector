package com.datastax.driver.spark.types

import java.util.Date

import org.apache.cassandra.serializers.TimestampSerializer

/** Formats timestamps and dates using CQL timestamp format `yyyy-MM-dd HH:mm:ssZ` */
object TimestampFormatter {
      
  private val serializer = TimestampSerializer.instance

  def format(date: Date): String = serializer.toString(date)
}
