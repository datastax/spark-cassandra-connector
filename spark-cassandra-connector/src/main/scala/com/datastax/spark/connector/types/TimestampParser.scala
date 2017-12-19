package com.datastax.spark.connector.types

import java.util.Date

import scala.collection.immutable.HashMap

import org.joda.time.format.DateTimeFormat

/** Parses CQL timestamps.
  *
  * Supported formats:
  *   - `yyyy-MM-dd HH:mm`
  *   - `yyyy-MM-dd HH:mmZ`
  *   - `yyyy-MM-dd HH:mm:ss`
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
  private val formatsBySize = HashMap[Int, String](
    4 -> "yyyy",
    10 -> "yyyy-MM-dd",
    15 -> "yyyy-MM-ddZ", // Z is 5 length, e.g. -0100
    16 -> "yyyy-MM-dd HH:mm",
    19 -> "yyyy-MM-dd HH:mm:ss",
    21 -> "yyyy-MM-dd HH:mmZ",
    24 -> "yyyy-MM-dd HH:mm:ssZ",
    23 -> "yyyy-MM-dd HH:mm:ss.SSS",
    28 -> "yyyy-MM-dd HH:mm:ss.SSSZ"
  ).mapValues(DateTimeFormat.forPattern)

  private val formatsWithTBySize = HashMap[Int, String](
    16 -> "yyyy-MM-dd'T'HH:mm",
    19 -> "yyyy-MM-dd'T'HH:mm:ss",
    21 -> "yyyy-MM-dd'T'HH:mmZ",
    24 -> "yyyy-MM-dd'T'HH:mm:ssZ",
    23 -> "yyyy-MM-dd'T'HH:mm:ss.SSS",
    28 -> "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  ).mapValues(DateTimeFormat.forPattern)

  def containsTAtFixedPosition(date: String): Boolean = {
    date.length > 10 && date.charAt(10) == 'T'
  }

  def parse(date: String): Date = {
    try {
      val format = if (containsTAtFixedPosition(date)) formatsWithTBySize(date.length) else formatsBySize(date.length)
      format.parseDateTime(date).toDate
    } catch {
      case _: Throwable => throw new IllegalArgumentException(s"Invalid date: [$date]")
    }
  }
}
