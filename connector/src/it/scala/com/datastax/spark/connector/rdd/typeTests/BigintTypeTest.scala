package com.datastax.spark.connector.rdd.typeTests

import java.lang.Long

class BigintTypeTest extends AbstractTypeTest[Long, Long] {
  override val typeName = "bigint"

  override val typeData: Seq[Long] = Seq(new Long(1000000L), new Long(2000000L), new Long(3000000L), new Long(4000000L), new Long(5000000L))
  override val addData: Seq[Long] = Seq(new Long(6000000000L), new Long(70000000L), new Long(80000000L), new Long(9000000L), new Long(10000000L))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Long = {
    row.getLong(colName)
  }
}

