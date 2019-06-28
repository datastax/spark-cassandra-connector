package com.datastax.spark.connector

import scala.util.Random

import org.joda.time.{DateTimeZone, LocalDate => JodaLocalDate}
import org.scalatest.{FlatSpec, Matchers}

import com.datastax.driver.core.LocalDate

class GettableDataSpec extends FlatSpec with Matchers {

  private def getLocalDates(count: Int): Seq[LocalDate] = for (t <- 1 to count) yield {
    Random.setSeed(10)
    val year = Random.nextInt(2000)
    val month = Random.nextInt(12) + 1
    val day = Random.nextInt(28) + 1
    LocalDate.fromYearMonthDay(year, month, day)
  }

  "GettableData" should "convert Driver LocalDates to Joda LocalDate" in {
    val zones = Seq(
      DateTimeZone.forID("America/New_York"),
      DateTimeZone.forID("Europe/Warsaw"),
      DateTimeZone.forID("+00:55"),
      DateTimeZone.forID("-00:33"),
      DateTimeZone.forID("UTC")
    )
    val dates = getLocalDates(count = 10000)

    val default = DateTimeZone.getDefault
    try {
      for (zone <- zones;
           date <- dates) {
        DateTimeZone.setDefault(zone)
        val jodaLocalDate = GettableData.convert(date).asInstanceOf[JodaLocalDate]
        val ld = (date.getYear, date.getMonth, date.getDay)
        val jd = (jodaLocalDate.getYear, jodaLocalDate.getMonthOfYear, jodaLocalDate.getDayOfMonth)

        withClue(s"LocalDate conversion failed for $zone, $date") {ld should be(jd)}
      }
    } finally {
      DateTimeZone.setDefault(default)
    }
  }
}
