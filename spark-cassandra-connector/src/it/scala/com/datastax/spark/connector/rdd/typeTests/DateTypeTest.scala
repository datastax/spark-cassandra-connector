package com.datastax.spark.connector.rdd.typeTests

import java.sql.Date

import com.datastax.driver.core.Row
import com.datastax.driver.core.LocalDate

class DateTypeTest extends AbstractTypeTest[Date, LocalDate]{
  override protected val typeName: String = "date"

  override def getDriverColumn(row: Row, colName: String): Date = {
    val ld = row.getDate(colName)
    Date.valueOf(ld.toString)
  }

  implicit def strToDate(str: String) : Date = java.sql.Date.valueOf(str)

  val dateRegx = """(\d\d\d\d)-(\d\d)-(\d\d)""".r

  override def convertToDriverInsertable(testValue: Date): LocalDate = {
    testValue.toString match { case dateRegx(year, month, day) =>
        LocalDate.fromYearMonthDay(year.toInt, month.toInt, day.toInt)
    }
  }

  override protected val typeData: Seq[Date] = Seq("2015-05-01", "2015-05-10", "2015-05-20")
  override protected val typeSet: Set[Date] = Set("1985-08-03", "1985-08-04", "1985-08-05")
  override protected val typeMap1: Map[String, Date] = Map("day1" -> "2000-01-01", "day2" -> "2000-01-02", "day3" -> "2000-01-03")
  override protected val typeMap2: Map[Date, String] = Map(strToDate("2000-02-01") -> "feb1", strToDate("2000-02-02") -> "feb2", strToDate("2000-02-03") -> "feb3")

  override protected val addData: Seq[Date] = Seq("2011-05-01", "2011-05-10", "2011-05-20")
  override protected val addSet: Set[Date] = Set("1989-08-03", "1989-08-04", "1989-08-05")
  override protected val addMap1: Map[String, Date] = Map("day1" -> "2000-08-03", "day2" -> "2000-08-04", "day3" -> "2000-08-05")
  override protected val addMap2: Map[Date, String] = Map(strToDate("2000-03-01") -> "mar1", strToDate("2000-03-02") -> "mar2", strToDate("2000-03-03") -> "mar3")
}
