package com.datastax.spark.connector.rdd.typeTests

import java.lang.Long

class BigintTypeTest extends AbstractTypeTest[Long, Long] {
  override val typeName = "bigint"
  override val typeData: Seq[Long] = Seq(new Long(1000000L), new Long(2000000L), new Long(3000000L), new Long(4000000L), new Long(5000000L))
  override val typeSet: Set[Long] = Set(new Long(101000L), new Long(102000L), new Long(103000L))
  override val typeMap1: Map[String, Long] = Map("key1" -> new Long(new Long(5000000L)), "key2" -> new Long(new Long(5001000L)), "key3" -> new Long(new Long(5002000L)))
  override val typeMap2: Map[Long, String] = Map(new Long(10000000L) -> "val1", new Long(10200000L) -> "val2", new Long(10300000L) -> "val3")

  override val addData: Seq[Long] = Seq(new Long(6000000000L), new Long(70000000L), new Long(80000000L), new Long(9000000L), new Long(10000000L))
  override val addSet: Set[Long] = Set(new Long(104000L), new Long(105000L), new Long(106000L))
  override val addMap1: Map[String, Long] = Map("key4" -> new Long(5003000L), "key5" -> new Long(5004000L), "key3" -> new Long(5005000L))
  override val addMap2: Map[Long, String] = Map(new Long(104000L) -> "val4", new Long(105000L) -> "val5", new Long(106000L) -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Long = {
    row.getLong(colName)
  }
}

