/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.rdd

import java.util.concurrent.LinkedTransferQueue

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.scalatest.concurrent.Eventually

import com.datastax.bdp.config.YamlClientConfiguration
import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.rdd.ConnectorMetricsListener.stagesMetrics

class ConnectorMetricsSpec extends DseITFlatSpecBase {

  import com.datastax.bdp.test.ng.DseAnalyticsTestUtils._

  YamlClientConfiguration.setAsClientConfigurationImpl()

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(
    sparkConf
      .set("spark.extraListeners", classOf[ConnectorMetricsListener].getName)
      .setMaster("local[16]")
      .setAppName(getClass.getSimpleName))

  override lazy val conn = CassandraConnector(sparkConf)

  beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
           |""".stripMargin)
      makeTables(session)
    }
  }

  def makeTables(session: Session): Unit = {
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $ks.leftjoin (
         |  key INT, 
         |  x INT, 
         |  PRIMARY KEY (key))
         |""".stripMargin)

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $ks.rightjoin (
         |  key INT, 
         |  y INT, 
         |  z INT, 
         |  PRIMARY KEY (key, y))
         |""".stripMargin)

    for (i <- 1 to 200) {
      session.execute(s"INSERT INTO $ks.leftjoin (key, x) values ($i, $i)")
      if ((i & 1) == 0) {
        for (j <- 1 to 200) {
          session.execute(s"INSERT INTO $ks.rightjoin (key, y, z) values ($i, $j, $i)")
        }
      }
    }
  }

  "InputMetricsUpdater" should "properly measure amount of data retrieved with CassandraTableScanRDD" in {
    stagesMetrics.clear()
    val rdd = sc.cassandraTable(ks, "leftjoin")
    rdd.withReadConf(rdd.readConf.copy(splitCount = Some(16))).collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200)
    metrics.inputMetrics.bytesRead should be(200 * 8)
  }

  it should "properly measure amount of data retrieved with CassandraJoinRDD" in {
    stagesMetrics.clear()
    val rdd = sc.cassandraTable(ks, "leftjoin")
    val joined = rdd.withReadConf(rdd.readConf.copy(splitCount = Some(16)))
      .joinWithCassandraTable(ks, "rightjoin", joinColumns = PartitionKeyColumns)
    joined.withReadConf(joined.readConf.copy(splitCount = Some(16))).collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200 + 100 * 200)
    metrics.inputMetrics.bytesRead should be(200 * 8 + 100 * 200 * 12)
  }

  it should "properly measure amount of data retrieved with CassandraLeftJoinRDD" in {
    stagesMetrics.clear()
    val rdd = sc.cassandraTable(ks, "leftjoin")
    val joined = rdd.withReadConf(rdd.readConf.copy(splitCount = Some(16)))
      .leftJoinWithCassandraTable(ks, "rightjoin", joinColumns = PartitionKeyColumns)
    joined.withReadConf(joined.readConf.copy(splitCount = Some(16))).collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200 + 100 * 200)
    metrics.inputMetrics.bytesRead should be(200 * 8 + 100 * 200 * 12)
  }

  it should "properly measure amount of data retrieved with CassandraMergeJoinRDD" in {
    stagesMetrics.clear()
    val left = sc.cassandraTable(ks, "leftjoin")
    val right = sc.cassandraTable(ks, "rightjoin")
    val joined = new CassandraMergeJoinRDD(
      sc,
      left.withReadConf(left.readConf.copy(splitCount = Some(16))),
      right.withReadConf(right.readConf.copy(splitCount = Some(16))))
    joined.collect()
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.inputMetrics.recordsRead should be(200 + 100 * 200)
    metrics.inputMetrics.bytesRead should be(200 * (8 + 8) + 100 * 200 * (12 + 8))
  }

  it should "properly measure amount of data written to Cassandra" in {
    stagesMetrics.clear()
    val rdd = sc.makeRDD(1 to 200, 16).map(x => (x, x))
    rdd.saveToCassandra(ks, "leftjoin")
    Eventually.eventually {
      stagesMetrics.size() should be(1)
    }
    val metrics = stagesMetrics.poll()
    metrics.outputMetrics.recordsWritten should be(200)
    metrics.outputMetrics.bytesWritten should be(200 * 8)
  }
}

class ConnectorMetricsListener(conf: SparkConf) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val metrics = stageCompleted.stageInfo.taskMetrics
    stagesMetrics.offer(metrics)
  }
}

object ConnectorMetricsListener {
  val stagesMetrics = new LinkedTransferQueue[TaskMetrics]()
}
