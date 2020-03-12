package com.datastax.spark.connector.types

import org.scalatest.FlatSpec

class TimestampParserSpec extends FlatSpec {

  it should "parse fast all supported date[time[zone]] formats" in {
    /* look in [[DateTimeFormatter]] for 'X' definition*/
    val validZones = Set(
      "",
      "Z",
      "-08",
      "-0830",
      "-08:30",
      "-083015",
      "-08:30:15",
      "+08",
      "+0830",
      "+08:30",
      "+083015",
      "+08:30:15"
    )

    val validDates = Set(
      "1986-01-02",
      "1986-01-02 21:05",
      "1986-01-02 21:05:07",
      "1986-01-02 21:05:07.1",
      "1986-01-02 21:05:07.12",
      "1986-01-02 21:05:07.123"
    )

    val datesAndDatesWithT = validDates
      .flatMap(date => Set(date) + date.replace(' ', 'T'))

    val allDates = for (date <- datesAndDatesWithT; zone <- validZones) yield {
      date + zone
    }

    allDates.foreach(TimestampParser.parseFastOrThrow)
  }
}
