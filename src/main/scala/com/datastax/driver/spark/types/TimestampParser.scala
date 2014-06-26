package com.datastax.driver.spark.types

import java.util.Date

import org.apache.cassandra.serializers.TimestampSerializer
import org.joda.time.format.DateTimeFormat

import scala.util.{Success, Try}

/** Parses CQL timestamps.
  *
  * Supported formats:
  *   - `yyyy-MM-dd HH:mm`
  *   - `yyyy-MM-dd HH:mm:ss`
  *   - `yyyy-MM-dd HH:mmZ`
  *   - `yyyy-MM-dd HH:mm:ssZ`
  *   - `yyyy-MM-dd HH:mm:ss.SSS`
  *   - `yyyy-MM-dd HH:mm:ss.SSSZ`
  *   - `yyyy-MM-dd'T'HH:mm`
  *   - `yyyy-MM-dd'T'HH:mmZ`
  *   - `yyyy-MM-dd'T'HH:mm:ss`
  *   - `yyyy-MM-dd'T'HH:mm:ssZ`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SSS`
  *   - `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
  *   - `yyyy-MM-dd`
  *   - `yyyy-MM-ddZ`
  */
object TimestampParser {

  private val parsers =
    TimestampSerializer.iso8601Patterns.map(DateTimeFormat.forPattern)

  def parse(date: String): Date = {
    parsers.view.map(p => Try(p.parseDateTime(date))).find(_.isSuccess) match {
      case Some(Success(d)) => d.toDate
      case _ => throw new IllegalArgumentException(s"Invalid date: $date")
    }
  }
}
