package com.datastax.spark.connector.rdd.typeTests


class DecimalTypeTest extends AbstractTypeTest[BigDecimal, java.math.BigDecimal] {

  implicit def toBigDecimal(str: String) = BigDecimal(str)

  override def convertToDriverInsertable(testValue: BigDecimal): java.math.BigDecimal = testValue.bigDecimal

  override val typeName = "decimal"
  override val typeData: Seq[BigDecimal] = Seq("100.1", "200.2","300.3", "400.4", "500.5")
  override val typeSet: Set[BigDecimal] = Set("101.1","102.2","103.3")
  override val typeMap1: Map[String, BigDecimal] = Map("key1" -> "1000.1", "key2" -> "2000.1", "key3" -> "3000.1")
  override val typeMap2: Map[BigDecimal, String] = Map(toBigDecimal("100") -> "val1", toBigDecimal("102") -> "val2", toBigDecimal("103") -> "val3")

  override val addData: Seq[BigDecimal] = Seq("600.6", "700.7", "800.8", "900.9", "1000.12")
  override val addSet: Set[BigDecimal] = Set("104.1985", "105.1985", "106.1985")
  override val addMap1: Map[String, BigDecimal] = Map("key4" -> "5003.2001", "key5" -> "5004.2002", "key3" -> "5005.2003")
  override val addMap2: Map[BigDecimal, String] = Map(toBigDecimal("104.444") -> "val4", toBigDecimal("105.444") -> "val5", toBigDecimal("106.444") -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): BigDecimal = {
    BigDecimal(row.getDecimal(colName))
  }
}
