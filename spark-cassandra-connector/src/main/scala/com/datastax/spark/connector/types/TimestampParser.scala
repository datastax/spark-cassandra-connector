package com.datastax.spark.connector.types

import java.util.Date

import org.joda.time.format.DateTimeFormat

import scala.util.{Success, Try}

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
  private val dateStringPatterns = Array[String](
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mmZ",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ssZ",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss.SSSZ",
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd'T'HH:mmZ",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    "yyyy-MM-dd",
    "yyyy-MM-ddZ")

  private val parsers =
    dateStringPatterns.map(DateTimeFormat.forPattern)

  def parse(date: String): Date = {
    parsers.view.map(p => Try(p.parseDateTime(date))).find(_.isSuccess) match {
      case Some(Success(d)) => d.toDate
      case _ => throw new IllegalArgumentException(s"Invalid date: $date")
    }
  }
}
