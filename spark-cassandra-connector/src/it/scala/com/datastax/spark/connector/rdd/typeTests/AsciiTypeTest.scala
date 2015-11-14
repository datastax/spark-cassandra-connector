package com.datastax.spark.connector.rdd.typeTests

class AsciiTypeTest extends AbstractTypeTest[String, String] {
  override val typeName = "ascii"

  override val typeData: Seq[String] = Seq("row1", "row2", "row3", "row4", "row5")
  override val addData: Seq[String] = Seq("row6", "row7", "row8", "row9", "row10")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): String = {
    row.getString(colName)
  }

}

