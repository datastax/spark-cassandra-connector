package com.datastax.spark.connector.rdd.typeTests

import java.lang.Double

class DoubleTypeTest extends AbstractTypeTest[Double, Double] {
  override val typeName = "double"

  override val typeData: Seq[Double] = Seq(new Double(100.1), new Double(200.2),new Double(300.3), new Double(400.4), new Double(500.5))
  override val addData: Seq[Double] = Seq(new Double(600.6), new Double(700.7), new Double(800.8), new Double(900.9), new Double(1000.12))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Double = {
    row.getDouble(colName)
  }
}

