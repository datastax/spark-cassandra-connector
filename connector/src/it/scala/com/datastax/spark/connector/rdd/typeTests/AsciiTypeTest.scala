package com.datastax.spark.connector.rdd.typeTests

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class AsciiTypeTest extends AbstractTypeTest[String, String] with DefaultCluster {
  override val typeName = "ascii"

  override val typeData: Seq[String] = Seq("row1", "row2", "row3", "row4", "row5")
  override val addData: Seq[String] = Seq("row6", "row7", "row8", "row9", "row10")

  override def getDriverColumn(row: Row, colName: String): String = {
    row.getString(colName)
  }

}

