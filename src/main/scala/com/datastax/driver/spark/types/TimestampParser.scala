package com.datastax.driver.spark.types

import java.util.Date

import org.apache.cassandra.serializers.TimestampSerializer
import org.joda.time.format.DateTimeFormat

import scala.util.{Success, Try}

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
