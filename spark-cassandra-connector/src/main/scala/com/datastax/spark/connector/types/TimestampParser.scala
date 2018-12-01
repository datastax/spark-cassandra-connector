package com.datastax.spark.connector.types

import java.util.Date

import scala.collection.immutable.HashMap
import scala.util.{Success, Try}

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

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
    22 -> "yyyy-MM-dd HH:mmZ",
    23 -> "yyyy-MM-dd HH:mm:ss.SSS",
    24 -> "yyyy-MM-dd HH:mm:ssZ",
    25 -> "yyyy-MM-dd HH:mm:ssZ",
    28 -> "yyyy-MM-dd HH:mm:ss.SSSZ",
    29 -> "yyyy-MM-dd HH:mm:ss.SSSZ"
  )

  private val formatsWithZBySize = HashMap[Int, String](
    11 -> "yyyy-MM-ddZ", // Z is 1 length here 'Z'
    17 -> "yyyy-MM-dd HH:mmZ",
    20 -> "yyyy-MM-dd HH:mm:ssZ",
    24 -> "yyyy-MM-dd HH:mm:ss.SSSZ"
  )

  /**
    * 3D Matrix: (hasT, hasZ, length) => DateTimeFormat
    *
    */
  private val converterMatrix = Array.tabulate[DateTimeFormatter](2, 2, formatsBySize.keys.max + 1) { (i, j, k) =>
    (if (j == 0) formatsBySize else formatsWithZBySize).get(k)
        .map(str => if (i == 1) str.replace(" ", "'T'") else str)
        .map(DateTimeFormat.forPattern).orNull
  }

  private val slowPathConverters = (formatsBySize.values ++ formatsBySize.values.map(_.replace(" ", "'T'")))
      .map(DateTimeFormat.forPattern)


  private def slowParse(date: String): Date = {
    slowPathConverters.view.map(p => Try(p.parseDateTime(date))).find(_.isSuccess) match {
      case Some(Success(d)) => d.toDate
      case _ => throw new IllegalArgumentException(s"Invalid date: $date")
    }
  }

  private def containsTAtFixedPosition(date: String): Boolean = {
    date.length > 10 && date.charAt(10) == 'T'
  }

  private def endsWithZ(date: String): Boolean = {
    date.length > 0 && date.charAt(date.length - 1) == 'Z'
  }

  def parse(date: String): Date = {
    try {
      implicit def bool2int(b: Boolean) = if (b) 1 else 0
      // trying guess string format by length and two symbols
      val format: DateTimeFormatter = converterMatrix(containsTAtFixedPosition(date))(endsWithZ(date))(date.length)
      format.parseDateTime(date).toDate
    } catch {
      // guess fail, iterate over all parsers
      case _: Throwable => slowParse(date)
    }
  }
}
