package com.datastax.spark.connector.datasource

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.Exchange

class CassandraCatalogTableReadSpec extends CassandraCatalogSpecBase {

  val testTable = "testTable"

  def setupBasicTable(): Unit = {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    val ps = conn.withSessionDo(_.prepare(s"""INSERT INTO $defaultKs."$testTable" (key, value) VALUES (?, ?)"""))
    awaitAll {
      for (i <- 0 to 100) yield {
        executor.executeAsync(ps.bind(i : java.lang.Integer, i.toString))
      }
    }
  }

  "A Cassandra Catalog Table Read Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should be(defaultCatalog)
  }

  it should "read from an empty table" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) USING cassandra PARTITIONED BY (key)")
    spark.sql(s"SELECT * FROM $defaultKs.$testTable").collect() shouldBe empty
  }

  it should "read from a table with some data" in {
    setupBasicTable()
    val results = spark.sql(s"SELECT * FROM $defaultKs.$testTable").collect()
    val expected = for (i <- 0 to 100) yield (i, i.toString)
    results.map( row => (row.getInt(0), row.getString(1))) should contain theSameElementsAs(expected)
  }

  it should "correctly use partitioning info" in {
    setupBasicTable()

    //Because partitioning supports this aggregate should not require a shuffle
    spark.sql(s"SELECT DISTINCT key FROM $defaultKs.$testTable")
      .queryExecution
      .executedPlan
      .collectFirst{ case exchange: Exchange => exchange } shouldBe empty

    //Because partitioning does not support this aggregate there should be a shuffle
    spark.sql(s"SELECT DISTINCT value FROM $defaultKs.$testTable")
      .queryExecution
      .executedPlan
      .collectFirst{ case exchange: Exchange => exchange } shouldBe defined
  }

  it should "handle count pushdowns" in {
    setupBasicTable()
    val request = spark.sql(s"""SELECT COUNT(*) from $defaultKs.$testTable""")
    val reader = request
      .queryExecution
      .executedPlan
      .collectFirst { case batchScanExec: BatchScanExec=> batchScanExec.readerFactory.createReader(EmptyInputPartition)}

    reader.get.isInstanceOf[CassandraCountPartitionReader] should be (true)
    request.collect()(0).get(0) should be (101)
  }

  it should "handle the programmtic api" in {
    setupBasicTable()
    val results = spark.read.table(s"$defaultKs.$testTable").collect()

    val expected = for (i <- 0 to 100) yield (i, i.toString)
    results.map( row => (row.getInt(0), row.getString(1))) should contain theSameElementsAs(expected)
  }

}
