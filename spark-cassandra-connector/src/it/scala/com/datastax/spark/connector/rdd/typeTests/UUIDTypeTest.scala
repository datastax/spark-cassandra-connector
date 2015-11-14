package com.datastax.spark.connector.rdd.typeTests

import java.util.UUID

class UUIDTypeTest extends AbstractTypeTest[UUID, UUID] {
  override val typeName = "uuid"
  override val typeData: Seq[UUID] = Seq(UUID.fromString("05FC7B82-758B-4FC6-91D1-0BD56911DFE8"), UUID.fromString("D27DEEBB-2573-4D2B-AB83-6CBB0708A688"),
    UUID.fromString("0B045E30-9BB4-4932-B11A-3B06FE1658C6"), UUID.fromString("0719091A-429D-4761-AB3D-3728E531718C"), UUID.fromString("DAE83E3D-C67E-4353-9D30-178A8CCBD7C9"))

  override val addData: Seq[UUID] = Seq(UUID.fromString("48830B99-F860-46A9-8187-31EC3F4F614A"), UUID.fromString("C8EF503C-EF97-479E-8E2E-FA363F7CEFD7"), UUID.fromString("77A07FDB-3ACC-4EEB-BEE4-DAE9388A3347"), UUID.fromString("89A6EC10-11F8-408A-A2AD-1A875A6D2E2B"), UUID.fromString("3B20E502-2993-42AE-A993-9425DBAB9EB1"))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): UUID = {
    row.getUUID(colName)
  }
}

