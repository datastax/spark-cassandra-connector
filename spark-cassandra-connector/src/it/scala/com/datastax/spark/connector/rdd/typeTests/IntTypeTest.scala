package com.datastax.spark.connector.rdd.typeTests

class IntTypeTest extends AbstractTypeTest[Integer, Integer] {
  override val typeName = "int"

  override val typeData: Seq[Integer] = Seq(new Integer(1), new Integer(2), new Integer(3), new Integer(4), new Integer(5))
  override val addData: Seq[Integer] = Seq(new Integer(6), new Integer(7), new Integer(8), new Integer(9), new Integer(10))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Integer = {
    row.getInt(colName)
  }

}

