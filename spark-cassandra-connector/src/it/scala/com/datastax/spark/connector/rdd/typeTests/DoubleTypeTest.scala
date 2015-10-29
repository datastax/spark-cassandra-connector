package com.datastax.spark.connector.rdd.typeTests

import java.lang.Double

class DoubleTypeTest extends AbstractTypeTest[Double, Double] {
  override val typeName = "double"
  override val typeData: Seq[Double] = Seq(new Double(100.1), new Double(200.2),new Double(300.3), new Double(400.4), new Double(500.5))
  override val typeSet: Set[Double] = Set(new Double(101.1),new Double(102.2),new Double(103.3))
  override val typeMap1: Map[String, Double] = Map("key1" -> new Double(1000.1), "key2" -> new Double(2000.1), "key3" -> new Double(3000.1))
  override val typeMap2: Map[Double, String] = Map(new Double(100) -> "val1", new Double(102) -> "val2", new Double(103) -> "val3")

  override val addData: Seq[Double] = Seq(new Double(600.6), new Double(700.7), new Double(800.8), new Double(900.9), new Double(1000.12))
  override val addSet: Set[Double] = Set(new Double(104.1985), new Double(105.1985), new Double(106.1985))
  override val addMap1: Map[String, Double] = Map("key4" -> new Double(5003.2001), "key5" -> new Double(5004.2002), "key3" -> new Double(5005.2003))
  override val addMap2: Map[Double, String] = Map(new Double(104.444) -> "val4", new Double(105.444) -> "val5", new Double(106.444) -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Double = {
    row.getDouble(colName)
  }
}

