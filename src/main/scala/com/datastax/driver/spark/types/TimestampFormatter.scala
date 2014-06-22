package com.datastax.driver.spark.types

import java.util.Date

import org.apache.cassandra.serializers.TimestampSerializer

object TimestampFormatter {
      
  private val serializer = TimestampSerializer.instance

  def format(date: Date): String = serializer.toString(date)
}
