package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.Row

class TinyIntTypeTest extends AbstractTypeTest[Int, java.lang.Byte] {
  override protected val typeName: String = "tinyint"

  override protected val typeData: Seq[Int] =Seq(1, 2, 3, 4, 5)
  override protected val addData: Seq[Int] = Seq(6, 7, 8, 9, 10)

  override def getDriverColumn(row: Row, colName: String): Int = {
    row.getByte(colName).toInt
  }

  override def convertToDriverInsertable(testValue: Int): java.lang.Byte = testValue.toByte


}
