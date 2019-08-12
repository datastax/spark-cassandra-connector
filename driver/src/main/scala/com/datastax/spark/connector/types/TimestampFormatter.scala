package com.datastax.spark.connector.types

import java.text.SimpleDateFormat
import java.util.Date

/** Formats timestamps and dates using CQL timestamp format `yyyy-MM-dd HH:mm:ssZ` */
object TimestampFormatter {

  private val TimestampPattern = "yyyy-MM-dd HH:mm:ssZ"

  def format(date: Date): String =
    new SimpleDateFormat(TimestampPattern).format(date)
}
