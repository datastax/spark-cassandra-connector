package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.types.ColumnTypeConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.{CassandraSourceRelation, TableRef}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DeprecationSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val conf = new SparkConf()

  conf.set("spark.cassandra.sql.pushdown.additionalClasses", "someClass")

  conf.set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
  conf.set("spark.cassandra.connection.ssl.enabledAlgorithms", "SUN-CARA")
  conf.set("spark.cassandra.connection.ssl.keyStore.path", "public")
  conf.set("spark.cassandra.connection.ssl.keyStore.password", "public")
  conf.set("spark.cassandra.connection.ssl.keyStore.type", "public")
  conf.set("spark.cassandra.connection.ssl.trustStore.path", "public")
  conf.set("spark.cassandra.connection.ssl.trustStore.password", "public")
  conf.set("spark.cassandra.connection.ssl.trustStore.type", "public")

  conf.set("spark.cassandra.dev.customFromDriver", "sundance")

  conf.set("spark.cassandra.input.join.throughput_query_per_sec", "42")

  conf.set("spark.cassandra.output.ifNotExists", "true")
  conf.set("spark.cassandra.output.ignoreNulls", "true")

  conf.set(CassandraSourceRelation.SolrPredciateOptimizationParam.name, "true")


  "The Spark Cassandra Connector" should "deprecate ConnectionConf" in {
    val cassandraConnectorConf = CassandraConnectorConf(conf)

    cassandraConnectorConf.cassandraSSLConf.clientAuthEnabled should be (true)
    cassandraConnectorConf.cassandraSSLConf.enabledAlgorithms should be (Set("SUN-CARA"))
    cassandraConnectorConf.cassandraSSLConf.keyStorePassword should contain ("public")
    cassandraConnectorConf.cassandraSSLConf.keyStorePath should contain ("public")
    cassandraConnectorConf.cassandraSSLConf.keyStoreType should be ("public")
    cassandraConnectorConf.cassandraSSLConf.trustStorePassword should contain ("public")
    cassandraConnectorConf.cassandraSSLConf.trustStorePath should contain ("public")
  }

  it should "deprecate WriteConf" in {
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.ignoreNulls should be (true)
    writeConf.ifNotExists should be (true)
  }

  it should "deprecate ReadConf" in {
    val readConf = ReadConf.fromSparkConf(conf)
    readConf.readsPerSec should be (42)
  }

  it should "deprecate SoureConf" in {
    val consolidateConf = CassandraSourceRelation.consolidateConfs(conf, Map.empty, TableRef("tab", "ks"), Map.empty)
    consolidateConf.get("spark.cassandra.sql.pushdown.additional_classes") should be ("someClass")
    consolidateConf.get(CassandraSourceRelation.SolrPredciateOptimizationParam.replacementParameter.get.name) should be ("true")

  }

  it should "deprecate ColumnTypeConf" in {
    val columnTypeConf = ColumnTypeConf.fromSparkConf(conf)
    columnTypeConf.customFromDriver.get should be("sundance")
  }
}
