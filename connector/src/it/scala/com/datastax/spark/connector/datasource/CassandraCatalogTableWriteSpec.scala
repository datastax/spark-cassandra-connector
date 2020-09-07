package com.datastax.spark.connector.datasource

import org.apache.spark.sql.functions._

class CassandraCatalogTableWriteSpec extends CassandraCatalogSpecBase {

  val testTable = "testTable"

  def setupBasicTable(): Unit = {
    createDefaultKs(1)
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    val ps = conn.withSessionDo(_.prepare(s"""INSERT INTO $defaultKs."$testTable" (key, value) VALUES (?, ?)"""))
    awaitAll {
      for (i <- 0 to 100) yield {
        executor.executeAsync(ps.bind(i : java.lang.Integer, i.toString))
      }
    }
  }

  "A Cassandra Catalog Table Write Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should be(defaultCatalog)
  }

  it should "support CTAS" in {
    val outputTable = "output_empty"
    setupBasicTable()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$outputTable USING cassandra PARTITIONED BY (key)
         |AS SELECT * FROM $defaultKs.$testTable""".stripMargin)
    val results = spark.sql(s"""SELECT * FROM $defaultKs.$outputTable""").collect()
    results.length shouldBe (101)
  }

  it should "support a vanilla insert" in {
    createDefaultKs(1)
    spark.sql(
      s"""CREATE TABLE $defaultKs.vanilla (key Int, value String) USING cassandra PARTITIONED BY (key)""".stripMargin)

    spark.range(0, 100)
      .withColumnRenamed("id", "key")
      .withColumn("value", col("key").cast("string"))
      .writeTo(s"$defaultKs.vanilla")
      .append()
    val results = spark.sql(s"""SELECT * FROM $defaultKs.vanilla""").collect()
    results.length shouldBe (100)
  }

  it should "report missing primary key columns" in intercept[CassandraCatalogException]{
    createDefaultKs(1)
    spark.sql(
      s"""CREATE TABLE $defaultKs.vanilla (key Int, value String) USING cassandra PARTITIONED BY (key)""".stripMargin)

    spark.range(0, 100)
      .withColumn("value", col("id").cast("string"))
      .writeTo(s"$defaultKs.vanilla")
      .append()
  }

}
