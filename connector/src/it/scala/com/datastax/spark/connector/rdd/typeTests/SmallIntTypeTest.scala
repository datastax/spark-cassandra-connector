package com.datastax.spark.connector.rdd.typeTests

import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class SmallIntTypeTest extends AbstractTypeTest[Short, java.lang.Short] with DefaultCluster {
  override val minPV = DefaultProtocolVersion.V4
  override protected val typeName: String = "smallint"

  override protected val typeData: Seq[Short] = (1 to 10).map(_.toShort)
  override protected val addData: Seq[Short] = (11 to 20).map(_.toShort)

  override def getDriverColumn(row: Row, colName: String): Short = row.getShort(colName)

}
