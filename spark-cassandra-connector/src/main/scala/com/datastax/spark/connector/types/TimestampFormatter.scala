package com.datastax.spark.connector.types

import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/** Formats timestamps and dates using CQL timestamp format `yyyy-MM-dd HH:mm:ssZ` */
object TimestampFormatter {

  private val TimestampPattern = "yyyy-MM-dd HH:mm:ssZ"

  def format(date: Date): String =
    DateTimeFormat.forPattern(TimestampPattern).print(new DateTime(date.getTime))
}
