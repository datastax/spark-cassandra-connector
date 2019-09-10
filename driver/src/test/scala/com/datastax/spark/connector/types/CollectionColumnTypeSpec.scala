package com.datastax.spark.connector.types

import org.scalatest.{FlatSpec, Matchers}

class CollectionColumnTypeSpec extends FlatSpec with Matchers {

  val udt = UserDefinedType(
    name = "address",
    columns = IndexedSeq(
      UDTFieldDef("street", VarCharType),
      UDTFieldDef("number", IntType)
    )
  )

  "ListType" should "mark nested UDT types as frozen" in {
    ListType(udt).cqlTypeName shouldBe "list<frozen<address>>"
  }

  it should "not mark non UDT types as frozen" in {
    ListType(IntType).cqlTypeName shouldBe "list<int>"
  }

  "SetType" should "mark nested UDT types as frozen" in {
    SetType(udt).cqlTypeName shouldBe "set<frozen<address>>"
  }

  it should "not mark non UDT types as frozen" in {
    SetType(IntType).cqlTypeName shouldBe "set<int>"
  }

  "MapType" should "mark key UDT types as frozen" in {
    MapType(udt, IntType).cqlTypeName shouldBe "map<frozen<address>, int>"
  }

  it should "mark value UDT types as frozen" in {
    MapType(IntType, udt).cqlTypeName shouldBe "map<int, frozen<address>>"
  }

}
