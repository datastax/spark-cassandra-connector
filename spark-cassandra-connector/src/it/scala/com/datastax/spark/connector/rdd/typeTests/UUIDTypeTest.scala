package com.datastax.spark.connector.rdd.typeTests

import java.util.UUID

class UUIDTypeTest extends AbstractTypeTest[UUID, UUID] {
  override val typeName = "uuid"
  override val typeData: Seq[UUID] = Seq(UUID.fromString("05FC7B82-758B-4FC6-91D1-0BD56911DFE8"), UUID.fromString("D27DEEBB-2573-4D2B-AB83-6CBB0708A688"),
    UUID.fromString("0B045E30-9BB4-4932-B11A-3B06FE1658C6"), UUID.fromString("0719091A-429D-4761-AB3D-3728E531718C"), UUID.fromString("DAE83E3D-C67E-4353-9D30-178A8CCBD7C9"))
  override val typeSet: Set[UUID] = Set(UUID.fromString("4D6EA85B-5054-4B05-90CA-69DB15E5D2C7"),UUID.fromString("0127A7BC-7341-47C0-8031-0A26ED6C9A05"),UUID.fromString("745D4DCA-861F-4543-9130-C0971AA5D62A"))
  override val typeMap1: Map[String, UUID] = Map("key1" -> UUID.fromString("A6E90573-3EA2-49D2-8ABC-C22F21A65769"), "key2" -> UUID.fromString("4FF9332E-835B-4C01-88DC-AD54CD3C6175"), "key3" -> UUID.fromString("5188968E-44FB-465F-8C74-F26AD7B8DF48"))
  override val typeMap2: Map[UUID, String] = Map(UUID.fromString("146BD6C2-40CB-4DEC-94A0-DEBBC3F03F0D") -> "val1", UUID.fromString("9D1E27E8-43FE-4F66-8E62-FAA1B5388BE0") -> "val2", UUID.fromString("848D2CE8-4A1B-48DA-BBB1-754747741C8D") -> "val3")

  override val addData: Seq[UUID] = Seq(UUID.fromString("48830B99-F860-46A9-8187-31EC3F4F614A"), UUID.fromString("C8EF503C-EF97-479E-8E2E-FA363F7CEFD7"), UUID.fromString("77A07FDB-3ACC-4EEB-BEE4-DAE9388A3347"), UUID.fromString("89A6EC10-11F8-408A-A2AD-1A875A6D2E2B"), UUID.fromString("3B20E502-2993-42AE-A993-9425DBAB9EB1"))
  override val addSet: Set[UUID] = Set(UUID.fromString("0F403157-2D20-47F0-B692-AEB78260902C"), UUID.fromString("022E7654-16F0-4450-B756-4DC000B4F51A"), UUID.fromString("D8460CC7-BA33-4D24-9F0D-D0255F5F3CBF"))
  override val addMap1: Map[String, UUID] = Map("key4" -> UUID.fromString("4362E3A5-2D11-4A5D-9A5D-647A2BC20DA1"), "key5" -> UUID.fromString("6D61ECA8-ED12-4FBE-A0C8-131BAD57487B"), "key3" -> UUID.fromString("A796DFF3-C959-45F3-BD43-C6085A1902D8"))
  override val addMap2: Map[UUID, String] = Map(UUID.fromString("C503235F-9EB5-4CFE-B0ED-BDC7103F7423") -> "val4", UUID.fromString("FF66253D-5144-4ABC-B3F4-6E28BA8CFF97") -> "val5", UUID.fromString("0C0254B6-560B-43F2-ACD3-ED2C89AEAFDB") -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): UUID = {
    row.getUUID(colName)
  }
}

