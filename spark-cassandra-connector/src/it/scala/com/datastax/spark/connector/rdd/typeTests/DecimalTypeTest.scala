package com.datastax.spark.connector.rdd.typeTests

import java.math.BigDecimal

class DecimalTypeTest extends AbstractTypeTest[BigDecimal, BigDecimal] {
  override val typeName = "decimal"
  override val typeData: Seq[BigDecimal] = Seq(new BigDecimal(100.1), new BigDecimal(200.2),new BigDecimal(300.3), new BigDecimal(400.4), new BigDecimal(500.5))
  override val typeSet: Set[BigDecimal] = Set(new BigDecimal(101.1),new BigDecimal(102.2),new BigDecimal(103.3))
  override val typeMap1: Map[String, BigDecimal] = Map("key1" -> new BigDecimal(1000.1), "key2" -> new BigDecimal(2000.1), "key3" -> new BigDecimal(3000.1))
  override val typeMap2: Map[BigDecimal, String] = Map(new BigDecimal(100) -> "val1", new BigDecimal(102) -> "val2", new BigDecimal(103) -> "val3")

  override val addData: Seq[BigDecimal] = Seq(new BigDecimal(600.6), new BigDecimal(700.7), new BigDecimal(800.8), new BigDecimal(900.9), new BigDecimal(1000.12))
  override val addSet: Set[BigDecimal] = Set(new BigDecimal(104.1985), new BigDecimal(105.1985), new BigDecimal(106.1985))
  override val addMap1: Map[String, BigDecimal] = Map("key4" -> new BigDecimal(5003.2001), "key5" -> new BigDecimal(5004.2002), "key3" -> new BigDecimal(5005.2003))
  override val addMap2: Map[BigDecimal, String] = Map(new BigDecimal(104.444) -> "val4", new BigDecimal(105.444) -> "val5", new BigDecimal(106.444) -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): BigDecimal = {
    row.getDecimal(colName)
  }
}

