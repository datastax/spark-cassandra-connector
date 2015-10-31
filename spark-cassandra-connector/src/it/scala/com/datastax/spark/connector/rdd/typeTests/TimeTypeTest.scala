package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.Row

class TimeTypeTest extends AbstractTypeTest[Long, java.lang.Long] {

  override def getDriverColumn(row: Row, colName: String): Long = row.getTime(colName)

  override protected val typeName: String = "time"

  override protected val typeData: Seq[Long] = 1L to 5L
  override protected val typeSet: Set[Long] = (1L to 3L).toSet
  override protected val typeMap1: Map[String, Long] = Map("key1" -> 1L, "key2" -> 2L, "key3" -> 3L)
  override protected val typeMap2: Map[Long, String] = Map(1L -> "val1", 2L -> "val2", 3L -> "val3")

  override protected val addData: Seq[Long] = 6L to 10L
  override protected val addSet: Set[Long] = (4L to 6L).toSet
  override protected val addMap1: Map[String, Long] = Map("key4" -> 4L, "key5" -> 5L, "key3" -> 6L)
  override protected val addMap2: Map[Long, String] = Map(4L -> "val4", 5L -> "val5", 6L -> "val6")

}
