package com.datastax.spark.connector.util

import com.datastax.spark.connector.TableRef
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.datasource.CassandraSourceUtil
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.types.ColumnTypeConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DeprecationSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val conf = new SparkConf()

  //Cassandra Connector Conf
  conf.set(CassandraConnectorConf.DeprecatedConnectionTimeoutParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedKeepAliveMillisParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedLocalDCParam.name, "otherdc")
  conf.set(CassandraConnectorConf.DeprecatedMaxReconnectionDelayParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedMinReconnectionDelayParam.name, "2500")
  conf.set(CassandraConnectorConf.DeprecatedReadTimeoutParam.name, "2500")

  //Read Conf
  conf.set(ReadConf.DeprecatedFetchSizeInRowsParam.name, "2500")
  conf.set(ReadConf.DeprecatedReadsPerSecParam.name, "2500")
  conf.set(ReadConf.DeprecatedSplitSizeInMBParam.name, "2500")

  //Write Conf
  conf.set(WriteConf.DeprecatedThroughputMiBPSParam.name, "2500")

  //CassandraSourceRelation
  conf.set(CassandraSourceRelation.SolrPredciateOptimizationParam.name, "true")


  "The Spark Cassandra Connector" should "deprecate ConnectionConf" in {
    val cassandraConnectorConf = CassandraConnectorConf(conf)
    cassandraConnectorConf.connectTimeoutMillis should be(2500)
    cassandraConnectorConf.keepAliveMillis should be (2500)
    cassandraConnectorConf.localDC should be (Some("otherdc"))
    cassandraConnectorConf.maxReconnectionDelayMillis should be (2500)
    cassandraConnectorConf.minReconnectionDelayMillis should be (2500)
    cassandraConnectorConf.readTimeoutMillis should be (2500)
  }

  it should "deprecate ReadConf" in {
    val readConf = ReadConf.fromSparkConf(conf)
    readConf.fetchSizeInRows should be (2500)
    readConf.readsPerSec.get should be (2500)
    readConf.splitSizeInMB should be (2500)
  }

  it should "deprecate WriteConf" in {
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.throughputMiBPS should be (Some(2500.0))
  }

  it should "deprecate SoureConf" in {
    val consolidateConf = CassandraSourceUtil.consolidateConfs(conf, Map.empty, "default", "ks")
    consolidateConf.get(CassandraSourceRelation.SolrPredciateOptimizationParam.replacementParameter.get.name) should be ("true")
  }

}
