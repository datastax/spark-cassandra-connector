package com.datastax.spark.connector.rdd.typeTests

import java.text.SimpleDateFormat
import java.util.Date

class TimestampTypeTest extends AbstractTypeTest[Date, Date] {
  override val typeName = "timestamp"
  val sdf = new SimpleDateFormat("dd/MM/yyyy")

  override val typeData: Seq[Date] = Seq(sdf.parse("03/08/1985"), sdf.parse("03/08/1986"),sdf.parse("03/08/1987"), sdf.parse("03/08/1988"), sdf.parse("03/08/1989"))
  override val addData: Seq[Date] = Seq(sdf.parse("03/08/1990"), sdf.parse("03/08/1991"), sdf.parse("03/08/1992"), sdf.parse("03/08/1993"), sdf.parse("03/08/1994"))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Date = {
    row.getTimestamp(colName)
  }

}

