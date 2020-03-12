/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.metastore

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster

class SparkSqlSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  private val tableName = "test_hive_partition" + System.currentTimeMillis()

  override def beforeClass {
    registerTable(tableName)
    insertDataIntoPartition(tableName, "2008-08-18", "red")
    insertDataIntoPartition(tableName, "2008-08-19", "blue")
    insertDataIntoPartition(tableName, "2008-08-20", "orange")
    insertDataIntoPartition(tableName, "2008-08-21", "yellow")
    insertDataIntoPartition(tableName, "2008-08-22", "yellow")
  }

  def registerTable(tableName: String): Unit = {
    val createDDL = s"CREATE TABLE IF NOT EXISTS $tableName (key String, value String) PARTITIONED BY (ds STRING, mn String)"
    spark.sql(createDDL)
  }

  def insertDataIntoPartition(tableName: String, dsPartition: String, mnPartition: String): Unit = {
    val loadData = s"INSERT INTO TABLE $tableName PARTITION (ds='$dsPartition', mn='$mnPartition') VALUES('1', '$mnPartition')"
    spark.sql(loadData)
  }

  def showPartitions(tableName: String, dsPartition: String, mnPartition: String): Int = {
    val partitionClause = if(dsPartition == null) {
        if (mnPartition == null) "" else s"PARTITION(mn='$mnPartition')"
      } else {
        if (mnPartition == null) s"PARTITION(ds ='$dsPartition')" else s"PARTITION(mn='$mnPartition', ds ='$dsPartition')"
      }
    val partitions = s"SHOW PARTITIONS $tableName $partitionClause"
    val result = spark.sql(partitions)
    result.collect().size
  }

  def selectDataFromPartition(tableName: String, dsPartition: String, mnPartition: String): Int ={
    val partitionClause = if(dsPartition == null) {
      if (mnPartition == null) "" else s"WHERE mn='$mnPartition'"
    } else {
      if (mnPartition == null) s"WHERE ds='$dsPartition'" else s"WHERE mn='$mnPartition' AND ds='$dsPartition'"
    }
    val select = s"SELECT DISTINCT key, value, ds, mn FROM $tableName $partitionClause"
    spark.sql(select).collect().size
  }

  "SHOW PARTITIONS" should "be able to show partitions" in {
    showPartitions(tableName, null, null) shouldBe 5
    showPartitions(tableName, "2008-08-18", null) shouldBe 1
    showPartitions(tableName, "2008-08-18", "black") shouldBe 0
    showPartitions(tableName, "2008-08-16", null) shouldBe 0
    showPartitions(tableName, null, "red") shouldBe 1
    showPartitions(tableName, null, "yellow") shouldBe 2
  }

  "Select data from partitions" should "be able to select data" in {
    selectDataFromPartition(tableName, null, null) shouldBe 5
    selectDataFromPartition(tableName, "2008-08-18", null) shouldBe 1
    // following assertion won't work in IDE if java assertions are not explicitly enabled
    an [Exception] should be thrownBy selectDataFromPartition(tableName, "2008-08-18", "black")
    selectDataFromPartition(tableName, "2008-08-16", null) shouldBe 0
    selectDataFromPartition(tableName, null, "red") shouldBe 1
    selectDataFromPartition(tableName, null, "yellow") shouldBe 2
  }
}
