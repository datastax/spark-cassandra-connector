package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.Row

class TinyIntTypeTest extends AbstractTypeTest[Int, java.lang.Byte] {
  override protected val typeName: String = "tinyint"

  /**
   * Java doesn't have the fun typeTests magic that Scala does so we'll need to use the driver
   * specific function for each expectedKeys typeTests
   */
  override def getDriverColumn(row: Row, colName: String): Int = {
    row.getByte(colName).toInt
  }

  override def convertToDriverInsertable(testValue: Int): java.lang.Byte = testValue.toByte

  override protected val typeData: Seq[Int] =Seq(1, 2, 3, 4, 5)
  override protected val typeSet: Set[Int] = Set(1, 2, 3)
  override protected val typeMap1: Map[String, Int] = Map("1" -> 1, "2" -> 2, "3" -> 3)
  override protected val typeMap2: Map[Int, String] = Map(1 -> "1", 2 -> "2", 3 -> "3")

  override protected val addData: Seq[Int] = Seq(6, 7, 8, 9, 10)
  override protected val addSet: Set[Int] = Set(4,5,6)
  override protected val addMap1: Map[String, Int] = Map("4" -> 4, "5" -> 5, "6" -> 6)
  override protected val addMap2: Map[Int, String] = Map(4 -> "4", 5 -> "5", 6 -> "6")

}
