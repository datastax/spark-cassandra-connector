package com.datastax.spark.connector.rdd.typeTests

import java.lang.Long

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class BigintTypeTest extends AbstractTypeTest[Long, Long] with DefaultCluster {
  override val typeName = "bigint"

  override val typeData: Seq[Long] = Seq(new Long(1000000L), new Long(2000000L), new Long(3000000L), new Long(4000000L), new Long(5000000L))
  override val addData: Seq[Long] = Seq(new Long(6000000000L), new Long(70000000L), new Long(80000000L), new Long(9000000L), new Long(10000000L))

  override def getDriverColumn(row: Row, colName: String): Long = {
    row.getLong(colName)
  }
}

