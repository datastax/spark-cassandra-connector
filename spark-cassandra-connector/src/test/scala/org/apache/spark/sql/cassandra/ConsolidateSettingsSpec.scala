package org.apache.spark.sql.cassandra

import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation._

class ConsolidateSettingsSpec extends FlatSpec with Matchers {

  it should "consolidate Cassandra conf settings in order of " +
    "table level -> keyspace -> cluster -> default" in {
    val tableRef = TableRef("table", "keyspace", Option("cluster"))
    val sparkConf = new SparkConf
    val rowSize = "spark.cassandra.input.page.row.size"

    sparkConf.set(rowSize, "10")
    val consolidatedConf =
      consolidateConfs(sparkConf, Map.empty, tableRef, Map.empty)
    consolidatedConf.get(rowSize) shouldBe "10"

    val sqlConf = Map[String, String](s"cluster/$rowSize" -> "100")
    val confWithClusterLevelSettings =
      consolidateConfs(sparkConf, sqlConf, tableRef, Map.empty)
    confWithClusterLevelSettings.get(rowSize) shouldBe "100"

    val sqlConf1 = sqlConf + (s"cluster:keyspace/$rowSize" -> "200")
    val confWithKeyspaceLevelSettings =
      consolidateConfs(sparkConf, sqlConf1, tableRef, Map.empty)
    confWithKeyspaceLevelSettings.get(rowSize) shouldBe "200"

    val tableConf = Map(rowSize -> "1000")
    val confWithTableLevelSettings =
      consolidateConfs(sparkConf, sqlConf1, tableRef, tableConf)
    confWithTableLevelSettings.get(rowSize) shouldBe "1000"
  }
}