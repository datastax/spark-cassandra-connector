package com.datastax.spark.connector.rdd.typeTests


class DecimalTypeTest extends AbstractTypeTest[BigDecimal, java.math.BigDecimal] {

  implicit def toBigDecimal(str: String) = BigDecimal(str)

  override def convertToDriverInsertable(testValue: BigDecimal): java.math.BigDecimal = testValue.bigDecimal

  override val typeName = "decimal"

  override val typeData: Seq[BigDecimal] = Seq("100.1", "200.2", "301.1")
  override val addData: Seq[BigDecimal] = Seq("600.6", "700.7", "721.444")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): BigDecimal = {
    BigDecimal(row.getDecimal(colName))
  }
}
