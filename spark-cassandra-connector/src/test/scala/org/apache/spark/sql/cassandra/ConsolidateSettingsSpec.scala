package org.apache.spark.sql.cassandra

import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation._

class ConsolidateSettingsSpec extends FlatSpec with Matchers {

  it should "consolidate Cassandra conf settings in order of table level -> keyspace -> cluster -> default" in {
    val tableRef = TableRef("table", "keyspace", Option("cluster"))
    val sparkConf = new SparkConf
    val rowSize = "spark.cassandra.input.page.row.size"

    sparkConf.set(rowSize, "10")
    consolidateConfs(sparkConf, Map.empty, tableRef, Map.empty).get(rowSize) shouldBe "10"

    val sqlConf = Map[String, String](s"cluster/$rowSize" -> "100")
    consolidateConfs(sparkConf, sqlConf, tableRef, Map.empty).get(rowSize) shouldBe "100"

    val sqlConf1 = sqlConf + (s"cluster:keyspace/$rowSize" -> "200")
    consolidateConfs(sparkConf, sqlConf1, tableRef, Map.empty).get(rowSize) shouldBe "200"

    val tableConf = Map(rowSize -> "1000")
    consolidateConfs(sparkConf, sqlConf1, tableRef, tableConf).get(rowSize) shouldBe "1000"
  }
}