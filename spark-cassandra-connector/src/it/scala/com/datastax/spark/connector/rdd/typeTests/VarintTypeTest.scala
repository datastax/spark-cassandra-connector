package com.datastax.spark.connector.rdd.typeTests

import java.math.BigInteger


class VarintTypeTest extends AbstractTypeTest[BigInteger, BigInteger] {
  override val typeName = "varint"

  override val typeData: Seq[BigInteger] = Seq(new BigInteger("1001"), new BigInteger("2002"),new BigInteger("3003"), new BigInteger("4004"), new BigInteger("5005"))
  override val addData: Seq[BigInteger] = Seq(new BigInteger("6006"), new BigInteger("7007"), new BigInteger("8008"), new BigInteger("9009"), new BigInteger("100012"))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): BigInteger = {
    row.getVarint(colName)
  }
}

