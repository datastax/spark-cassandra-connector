package org.apache.spark.sql.cassandra

import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.scalatest.{FlatSpec, Matchers}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf

class ConsolidateSettingsSpec extends FlatSpec with Matchers {

  val param = ReadConf.FetchSizeInRowsParam
  val sparkConf = new SparkConf(loadDefaults = false)
  val tableRef = TableRef("table", "keyspace", Option("cluster"))
  val tableRefDefaultCluster = TableRef("table", "keyspace")

  def verify(
      sparkConf: Map[String, String],
      sqlContextConf: Map[String, String],
      tableConf: Map[String, String],
      value: Option[String],
      valueForDefaultCluster: Option[String],
      checkParam: String = param.name): Unit = {
    val sc = this.sparkConf.clone().setAll(sparkConf)

    val consolidatedConf1 = consolidateConfs(sc, sqlContextConf, tableRef, tableConf)
    val consolidatedConf2 = consolidateConfs(sc, sqlContextConf, tableRefDefaultCluster, Map.empty)
    consolidatedConf1.getOption(checkParam) shouldBe value
    consolidatedConf2.getOption(checkParam) shouldBe valueForDefaultCluster
  }

  it should "use SparkConf settings by default" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map.empty,
      tableConf = Map.empty,
      value = Some("10"),
      valueForDefaultCluster = Some("10"))
  }

  it should "override SparkConf settings by SQLContext settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(s"default/${param.name}" -> "20"),
      tableConf = Map.empty,
      value = Some("20"),
      valueForDefaultCluster = Some("20"))
  }

  it should "override global SQLContext settings by cluster level settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30"),
      tableConf = Map.empty,
      value = Some("30"),
      valueForDefaultCluster = Some("20"))
  }

  it should "override cluster level SQLContext settings by keyspace level settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30",
        s"${tableRef.cluster.get}:${tableRef.keyspace}/${param.name}" -> "40"),
      tableConf = Map.empty,
      value = Some("40"),
      valueForDefaultCluster = Some("20"))
  }

  it should "override keyspace level SQLContext settings by table level settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30",
        s"${tableRef.cluster.get}:${tableRef.keyspace}/${param.name}" -> "40"),
      tableConf = Map(param.name -> "50"),
      value = Some("50"),
      valueForDefaultCluster = Some("20"))
  }

  it should "accept sqlConf settings without a cluster set" in {
    verify(
      sparkConf = Map.empty,
      sqlContextConf = Map(param.name -> "20"),
      tableConf = Map.empty,
      value = Some("20"),
      valueForDefaultCluster = Some("20"))
  }

}
