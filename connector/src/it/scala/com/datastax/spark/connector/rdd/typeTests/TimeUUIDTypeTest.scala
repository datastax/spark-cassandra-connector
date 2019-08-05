package com.datastax.spark.connector.rdd.typeTests

import java.util.UUID

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class TimeUUIDTypeTest extends AbstractTypeTest[UUID, UUID] with DefaultCluster {

  override val typeName = "timeuuid"

  override val typeData: Seq[UUID] = Seq(UUID.fromString("61129590-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129591-FBE4-11E3-A3AC-0800200C9A66"),
    UUID.fromString("61129592-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129593-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129594-FBE4-11E3-A3AC-0800200C9A66"))
  override val addData: Seq[UUID] = Seq(UUID.fromString("204FF380-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF381-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF382-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF383-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF384-FBE5-11E3-A3AC-0800200C9A66"))

  override def getDriverColumn(row: Row, colName: String): UUID = {
    row.getUuid(colName)
  }
}

