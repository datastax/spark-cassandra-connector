package com.datastax.spark.connector.rdd.typeTests

import java.lang.Float

class FloatTypeTest extends AbstractTypeTest[Float, Float] {
  override val typeName = "float"
  override val typeData: Seq[Float] = Seq(new Float(100.1), new Float(200.2),new Float(300.3), new Float(400.4), new Float(500.5))
  override val typeSet: Set[Float] = Set(new Float(101.1),new Float(102.2),new Float(103.3))
  override val typeMap1: Map[String, Float] = Map("key1" -> new Float(1000.1), "key2" -> new Float(2000.1), "key3" -> new Float(3000.1))
  override val typeMap2: Map[Float, String] = Map(new Float(100) -> "val1", new Float(102) -> "val2", new Float(103) -> "val3")

  override val addData: Seq[Float] = Seq(new Float(600.6), new Float(700.7), new Float(800.8), new Float(900.9), new Float(1000.12))
  override val addSet: Set[Float] = Set(new Float(104.1985), new Float(105.1985), new Float(106.1985))
  override val addMap1: Map[String, Float] = Map("key4" -> new Float(5003.2001), "key5" -> new Float(5004.2002), "key3" -> new Float(5005.2003))
  override val addMap2: Map[Float, String] = Map(new Float(104.444) -> "val4", new Float(105.444) -> "val5", new Float(106.444) -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Float = {
    row.getFloat(colName)
  }

}

