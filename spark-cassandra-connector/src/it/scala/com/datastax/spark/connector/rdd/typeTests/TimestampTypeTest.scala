package com.datastax.spark.connector.rdd.typeTests

import java.text.SimpleDateFormat
import java.util.Date

class TimestampTypeTest extends AbstractTypeTest[Date, Date] {
  override val typeName = "timestamp"
  val sdf = new SimpleDateFormat("dd/MM/yyyy")

  override val typeData: Seq[Date] = Seq(sdf.parse("03/08/1985"), sdf.parse("03/08/1986"),sdf.parse("03/08/1987"), sdf.parse("03/08/1988"), sdf.parse("03/08/1989"))
  override val typeSet: Set[Date] = Set(sdf.parse("30/05/1987"),sdf.parse("30/05/1988"),sdf.parse("03/08/1989"))
  override val typeMap1: Map[String, Date] = Map("key1" -> sdf.parse("01/01/0001"), "key2" -> sdf.parse("15/05/2000"), "key3" -> sdf.parse("01/01/2010"))
  override val typeMap2: Map[Date, String] = Map(sdf.parse("24/12/2003") -> "val1", sdf.parse("24/12/2004") -> "val2", sdf.parse("24/12/2005") -> "val3")

  override val addData: Seq[Date] = Seq(sdf.parse("03/08/1990"), sdf.parse("03/08/1991"), sdf.parse("03/08/1992"), sdf.parse("03/08/1993"), sdf.parse("03/08/1994"))
  override val addSet: Set[Date] = Set(sdf.parse("03/03/2003"), sdf.parse("04/04/2004"), sdf.parse("05/05/2005"))
  override val addMap1: Map[String, Date] = Map("key4" -> sdf.parse("06/06/2006"), "key5" -> sdf.parse("07/07/2007"), "key3" -> sdf.parse("08/08/2008"))
  override val addMap2: Map[Date, String] = Map(sdf.parse("01/01/2001") -> "val4", sdf.parse("01/01/2002") -> "val5", sdf.parse("01/01/2003") -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Date = {
    row.getTimestamp(colName)
  }

}

