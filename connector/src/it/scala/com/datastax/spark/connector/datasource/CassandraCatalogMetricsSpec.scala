/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.datasource

import scala.collection.mutable
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.BeforeAndAfterEach
import com.datastax.spark.connector.datasource.CassandraCatalog
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession


class CassandraCatalogMetricsSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with BeforeAndAfterEach {

  override lazy val conn = CassandraConnector(defaultConf)

  override lazy val spark = SparkSession.builder()
    .config(sparkConf
      // Enable Codahale/Dropwizard metrics
      .set("spark.metrics.conf.executor.source.cassandra-connector.class", "org.apache.spark.metrics.CassandraConnectorSource")
      .set("spark.metrics.conf.driver.source.cassandra-connector.class", "org.apache.spark.metrics.CassandraConnectorSource")
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.defaultCatalog", "cassandra")
      .set("spark.sql.catalog.cassandra", classOf[CassandraCatalog].getCanonicalName)
    )
    .withExtensions(new CassandraSparkExtensions).getOrCreate().newSession()

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $ks WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $ks.leftjoin (key INT, x INT, PRIMARY KEY (key))")
      for (i <- 1 to 1000 * 10) {
        session.execute(s"INSERT INTO $ks.leftjoin (key, x) values ($i, $i)")
      }
    }
  }

  var readRowCount: Long = 0
  var readByteCount: Long = 0

  it should "update Codahale read metrics for SELECT queries" in {
    val df = spark.sql(s"SELECT x FROM $ks.leftjoin LIMIT 2")
    val metricsRDD = df.queryExecution.toRdd.mapPartitions { iter =>
      val tc = org.apache.spark.TaskContext.get()
      val source = org.apache.spark.metrics.MetricsUpdater.getSource(tc)
      Iterator((source.get.readRowMeter.getCount, source.get.readByteMeter.getCount))
    }

    val metrics = metricsRDD.collect()
    readRowCount = metrics.map(_._1).sum - readRowCount
    readByteCount = metrics.map(_._2).sum - readByteCount

    assert(readRowCount > 0)
    assert(readByteCount == readRowCount * 4) // 4 bytes per INT result
  }

  it should "update Codahale read metrics for COUNT queries" in {
    val df = spark.sql(s"SELECT COUNT(*) FROM $ks.leftjoin")
    val metricsRDD = df.queryExecution.toRdd.mapPartitions { iter =>
      val tc = org.apache.spark.TaskContext.get()
      val source = org.apache.spark.metrics.MetricsUpdater.getSource(tc)
      Iterator((source.get.readRowMeter.getCount, source.get.readByteMeter.getCount))
    }

    val metrics = metricsRDD.collect()
    readRowCount = metrics.map(_._1).sum - readRowCount
    readByteCount = metrics.map(_._2).sum - readByteCount

    assert(readRowCount > 0)
    assert(readByteCount == readRowCount * 8) // 8 bytes per COUNT result
  }
}
