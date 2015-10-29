package com.datastax.spark.connector.rdd.typeTests

class IntTypeTest extends AbstractTypeTest[Integer, Integer] {
  override val typeName = "int"
  override val typeData: Seq[Integer] = Seq(new Integer(1), new Integer(2), new Integer(3), new Integer(4), new Integer(5))
  override val typeSet: Set[Integer] = Set(new Integer(101), new Integer(102), new Integer(103))
  override val typeMap1: Map[String, Integer] = Map("key1" -> new Integer(5000), "key2" -> new Integer(5001), "key3" -> new Integer(5002))
  override val typeMap2: Map[Integer, String] = Map(new Integer(100) -> "val1", new Integer(102) -> "val2", new Integer(103) -> "val3")

  override val addData: Seq[Integer] = Seq(new Integer(6), new Integer(7), new Integer(8), new Integer(9), new Integer(10))
  override val addSet: Set[Integer] = Set(new Integer(104), new Integer(105), new Integer(106))
  override val addMap1: Map[String, Integer] = Map("key4" -> new Integer(5003), "key5" -> new Integer(5004), "key3" -> new Integer(5005))
  override val addMap2: Map[Integer, String] = Map(new Integer(104) -> "val4", new Integer(105) -> "val5", new Integer(106) -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Integer = {
    row.getInt(colName)
  }

}

