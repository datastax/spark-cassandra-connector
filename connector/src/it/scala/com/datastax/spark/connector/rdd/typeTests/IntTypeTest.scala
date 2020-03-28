package com.datastax.spark.connector.rdd.typeTests

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class IntTypeTest extends AbstractTypeTest[Integer, Integer] with DefaultCluster {
  override val typeName = "int"

  override val typeData: Seq[Integer] = Seq(new Integer(1), new Integer(2), new Integer(3), new Integer(4), new Integer(5))
  override val addData: Seq[Integer] = Seq(new Integer(6), new Integer(7), new Integer(8), new Integer(9), new Integer(10))

  override def getDriverColumn(row: Row, colName: String): Integer = {
    row.getInt(colName)
  }

}

