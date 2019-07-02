package com.datastax.spark.connector.rdd.typeTests

import java.lang.Float

class FloatTypeTest extends AbstractTypeTest[Float, Float] {
  override val typeName = "float"

  override val typeData: Seq[Float] = Seq(new Float(100.1), new Float(200.2),new Float(300.3), new Float(400.4), new Float(500.5))
  override val addData: Seq[Float] = Seq(new Float(600.6), new Float(700.7), new Float(800.8), new Float(900.9), new Float(1000.12))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Float = {
    row.getFloat(colName)
  }

}

