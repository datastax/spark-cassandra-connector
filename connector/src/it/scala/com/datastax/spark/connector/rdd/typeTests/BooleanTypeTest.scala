package com.datastax.spark.connector.rdd.typeTests

import java.lang.Boolean

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class BooleanTypeTest extends AbstractTypeTest[Boolean, Boolean] with DefaultCluster {
  override val typeName = "boolean"

  override val typeData: Seq[Boolean] = Seq(new Boolean(true))
  override val addData: Seq[Boolean] = Seq(new Boolean(false))

  override def getDriverColumn(row: Row, colName: String): Boolean = {
    row.getBoolean(colName)
  }

}

