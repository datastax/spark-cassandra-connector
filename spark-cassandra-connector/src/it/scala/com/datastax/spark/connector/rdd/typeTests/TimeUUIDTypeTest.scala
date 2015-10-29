package com.datastax.spark.connector.rdd.typeTests

import java.util.UUID

class TimeUUIDTypeTest extends AbstractTypeTest[UUID, UUID] { override val typeName = "timeuuid"
  override val typeData: Seq[UUID] = Seq(UUID.fromString("61129590-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129591-FBE4-11E3-A3AC-0800200C9A66"),
    UUID.fromString("61129592-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129593-FBE4-11E3-A3AC-0800200C9A66"), UUID.fromString("61129594-FBE4-11E3-A3AC-0800200C9A66"))
  override val typeSet: Set[UUID] = Set(UUID.fromString("61129595-FBE4-11E3-A3AC-0800200C9A66"),UUID.fromString("61129596-FBE4-11E3-A3AC-0800200C9A66"),UUID.fromString("61129597-FBE4-11E3-A3AC-0800200C9A66"))
  override val typeMap1: Map[String, UUID] = Map("key1" -> UUID.fromString("61129598-FBE4-11E3-A3AC-0800200C9A66"), "key2" -> UUID.fromString("61129599-FBE4-11E3-A3AC-0800200C9A66"), "key3" -> UUID.fromString("6112959A-FBE4-11E3-A3AC-0800200C9A66"))
  override val typeMap2: Map[UUID, String] = Map(UUID.fromString("6112959B-FBE4-11E3-A3AC-0800200C9A66") -> "val1", UUID.fromString("6112959C-FBE4-11E3-A3AC-0800200C9A66") -> "val2", UUID.fromString("6112959D-FBE4-11E3-A3AC-0800200C9A66") -> "val3")

  override val addData: Seq[UUID] = Seq(UUID.fromString("204FF380-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF381-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF382-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF383-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF384-FBE5-11E3-A3AC-0800200C9A66"))
  override val addSet: Set[UUID] = Set(UUID.fromString("204FF385-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF386-FBE5-11E3-A3AC-0800200C9A66"), UUID.fromString("204FF387-FBE5-11E3-A3AC-0800200C9A66"))
  override val addMap1: Map[String, UUID] = Map("key4" -> UUID.fromString("204FF388-FBE5-11E3-A3AC-0800200C9A66"), "key5" -> UUID.fromString("204FF389-FBE5-11E3-A3AC-0800200C9A66"), "key3" -> UUID.fromString("204FF38A-FBE5-11E3-A3AC-0800200C9A66"))
  override val addMap2: Map[UUID, String] = Map(UUID.fromString("204FF38B-FBE5-11E3-A3AC-0800200C9A66") -> "val4", UUID.fromString("204FF38C-FBE5-11E3-A3AC-0800200C9A66") -> "val5", UUID.fromString("204FF38D-FBE5-11E3-A3AC-0800200C9A66") -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): UUID = {
    row.getUUID(colName)
  }
}

