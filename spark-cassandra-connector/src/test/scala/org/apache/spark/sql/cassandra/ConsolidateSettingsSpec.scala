package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.embedded.SparkTemplate
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.sql.cassandra.CassandraSourceRelation._

class ConsolidateSettingsSpec extends FlatSpec with Matchers {

  it should "consolidate Cassandra conf settings in order of table level -> keyspace -> cluster -> default" in {
    val tableRef = TableRef("table", "keyspace", Option("cluster"))
    val conf = SparkTemplate.defaultConf
    val rowSize = "spark.cassandra.input.page.row.size"
    conf.set(rowSize, "10")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    consolidateConfs(sqlContext, tableRef, Map.empty).get(rowSize) shouldBe "10"
    sqlContext.setConf(s"cluster/$rowSize", "100")
    consolidateConfs(sqlContext, tableRef, Map.empty).get(rowSize) shouldBe "100"
    sqlContext.setConf(s"cluster:keyspace/$rowSize", "200")
    consolidateConfs(sqlContext, tableRef, Map.empty).get(rowSize) shouldBe "200"
    val tableConf = Map(rowSize -> "1000")
    consolidateConfs(sqlContext, tableRef, tableConf).get(rowSize) shouldBe "1000"
  }
}
