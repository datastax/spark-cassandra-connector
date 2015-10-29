package com.datastax.spark.connector.rdd.typeTests

import java.math.BigInteger


class VarintTypeTest extends AbstractTypeTest[BigInteger, BigInteger] {
  override val typeName = "varint"

  override val typeData: Seq[BigInteger] = Seq(new BigInteger("1001"), new BigInteger("2002"),new BigInteger("3003"), new BigInteger("4004"), new BigInteger("5005"))
  override val typeSet: Set[BigInteger] = Set(new BigInteger("1011"),new BigInteger("1022"),new BigInteger("1033"))
  override val typeMap1: Map[String, BigInteger] = Map("key1" -> new BigInteger("10001"), "key2" -> new BigInteger("20001"), "key3" -> new BigInteger("30001"))
  override val typeMap2: Map[BigInteger, String] = Map(new BigInteger("100") -> "val1", new BigInteger("102") -> "val2", new BigInteger("103") -> "val3")

  override val addData: Seq[BigInteger] = Seq(new BigInteger("6006"), new BigInteger("7007"), new BigInteger("8008"), new BigInteger("9009"), new BigInteger("100012"))
  override val addSet: Set[BigInteger] = Set(new BigInteger("1041985"), new BigInteger("1051985"), new BigInteger("1061985"))
  override val addMap1: Map[String, BigInteger] = Map("key4" -> new BigInteger("50032001"), "key5" -> new BigInteger("50042002"), "key3" -> new BigInteger("50052003"))
  override val addMap2: Map[BigInteger, String] = Map(new BigInteger("104444") -> "val4", new BigInteger("105444") -> "val5", new BigInteger("106444") -> "val6")

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): BigInteger = {
    row.getVarint(colName)
  }
}

