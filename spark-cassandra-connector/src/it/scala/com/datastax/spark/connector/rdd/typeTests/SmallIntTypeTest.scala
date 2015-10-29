package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.Row

class SmallIntTypeTest extends AbstractTypeTest[Short, java.lang.Short]{
  override protected val typeName: String = "smallint"

  /**
   * Java doesn't have the fun typeTests magic that Scala does so we'll need to use the driver
   * specific function for each expectedKeys typeTests
   */
  override def getDriverColumn(row: Row, colName: String): Short = row.getShort(colName)

  override protected val typeData: Seq[Short] = (1 to 10).map(_.toShort)
  override protected val typeSet: Set[Short] = (1 to 3).map(_.toShort).toSet
  override protected val typeMap1: Map[String, Short] = Map("key1" -> 1, "key2" -> 2, "key3" -> 3)
  override protected val typeMap2: Map[Short, String] = Map(1.toShort -> "val1", 2.toShort -> "val2", 3.toShort -> "val3")

  override protected val addData: Seq[Short] = (11 to 20).map(_.toShort)
  override protected val addSet: Set[Short] = (4 to 6).map(_.toShort).toSet
  override protected val addMap1: Map[String, Short] = Map("key4" -> 4, "key5" -> 5, "key6" -> 6)
  override protected val addMap2: Map[Short, String] = Map(4.toShort -> "val1", 5.toShort -> "val2", 6.toShort -> "val3")

}
