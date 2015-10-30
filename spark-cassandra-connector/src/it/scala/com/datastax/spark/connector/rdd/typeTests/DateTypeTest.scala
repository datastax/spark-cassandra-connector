package com.datastax.spark.connector.rdd.typeTests

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.datastax.driver.core.Row
import com.datastax.driver.core.LocalDate
import java.sql.Date

class DateTypeTest extends AbstractTypeTest[Date, LocalDate]{
  override protected val typeName: String = "date"

  override def getDriverColumn(row: Row, colName: String): Date =
    new Date(row.getDate(colName).getMillisSinceEpoch)

  override def dataframeReadWrite(): Unit = {
    markup("Until https://issues.apache.org/jira/browse/SPARK-11415 is complete Dataframe Date is broken")
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd:Z")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  implicit def strToDate(str: String) : Date = new java.sql.Date(sdf.parse(str+":-0000").getTime)

  override def convertToDriverInsertable(testValue: Date): LocalDate =
    LocalDate.fromMillisSinceEpoch(testValue.getTime)

  override protected val typeData: Seq[Date] = Seq("2015-05-01", "2015-05-010", "2015-05-20")
  override protected val typeSet: Set[Date] = Set("1985-08-03", "1985-08-04", "1985-08-05")
  override protected val typeMap1: Map[String, Date] = Map("day1" -> "2000-01-01", "day2" -> "2000-01-02", "day3" -> "2000-01-03")
  override protected val typeMap2: Map[Date, String] = Map(strToDate("2000-02-01") -> "feb1", strToDate("2000-02-02") -> "feb2", strToDate("2000-02-03") -> "feb3")

  override protected val addData: Seq[Date] = Seq("2011-05-01", "2011-05-10", "2011-05-20")
  override protected val addSet: Set[Date] = Set("1989-08-03", "1989-08-04", "1989-08-05")
  override protected val addMap1: Map[String, Date] = Map("day1" -> "2000-08-03", "day2" -> "2000-08-04", "day3" -> "2000-08-05")
  override protected val addMap2: Map[Date, String] = Map(strToDate("2000-03-01") -> "mar1", strToDate("2000-03-02") -> "mar2", strToDate("2000-03-03") -> "mar3")
}
