package com.datastax.spark.connector.rdd.typeTests

import java.math.BigDecimal
import java.math.BigInteger

import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.SaveMode

case class LongRow(pkey: Long, ckey1: Long, ckey2: Long, data1: Long)
case class BigDecimalRow(pkey: BigDecimal, ckey1: BigDecimal, ckey2: BigDecimal, data1: BigDecimal)

class VarintTypeTest extends AbstractTypeTest[BigInteger, BigInteger] {
  override val typeName = "varint"

  override val typeData: Seq[BigInteger] = Seq(new BigInteger("1001"), new BigInteger("2002"),new BigInteger("3003"), new BigInteger("4004"), new BigInteger("5005"))
  override val addData: Seq[BigInteger] = Seq(new BigInteger("6006"), new BigInteger("7007"), new BigInteger("8008"), new BigInteger("9009"), new BigInteger("100012"))

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): BigInteger = {
    row.getVarint(colName)
  }

  "A LongType DataFrame" should "write to VarInt C* tables" in {
    val LongValue = 11111111
    val longRDD = sc.parallelize(Seq(LongRow(LongValue, LongValue, LongValue, LongValue)))
    val longDf = sparkSession.createDataFrame(longRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    longDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = $LongValue and ckey1 = $LongValue").one)
    row.getVarint("data1").longValue() should be (LongValue)

  }

  "A DecimalType DataFrame with Scale 0" should "write to VarInt C* Tables" in {
    val BigDecimalValue = new BigDecimal(22222)

    val bigDecimalRDD = sc.parallelize(Seq(BigDecimalRow(BigDecimalValue, BigDecimalValue, BigDecimalValue, BigDecimalValue)))
    val bigDecimalDf = sparkSession.createDataFrame(bigDecimalRDD)

    val normOptions = Map("keyspace" -> keyspaceName, "table" -> typeNormalTable)
    bigDecimalDf
      .withColumn("pkey", bigDecimalDf("pkey").cast(DecimalType(12,0)))
      .withColumn("ckey1", bigDecimalDf("ckey2").cast(DecimalType(12,0)))
      .withColumn("ckey2", bigDecimalDf("ckey2").cast(DecimalType(12,0)))
      .withColumn("data1", bigDecimalDf("data1").cast(DecimalType(12,0)))
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(normOptions)
      .mode(SaveMode.Append)
      .save()

    val row = conn.withSessionDo(session =>
      session.execute(s"SELECT * FROM $keyspaceName.$typeNormalTable WHERE pkey = $BigDecimalValue and ckey1 = $BigDecimalValue").one)
    row.getVarint("data1").longValue() should be (BigDecimalValue.longValue())
  }
}

