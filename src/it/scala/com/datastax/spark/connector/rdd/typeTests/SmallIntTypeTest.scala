package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.{ProtocolVersion, Row}

class SmallIntTypeTest extends AbstractTypeTest[Short, java.lang.Short]{
  override val minPV = ProtocolVersion.V4
  override protected val typeName: String = "smallint"

  override protected val typeData: Seq[Short] = (1 to 10).map(_.toShort)
  override protected val addData: Seq[Short] = (11 to 20).map(_.toShort)

  override def getDriverColumn(row: Row, colName: String): Short = row.getShort(colName)

}
