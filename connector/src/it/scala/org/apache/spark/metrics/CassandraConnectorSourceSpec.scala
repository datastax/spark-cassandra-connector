package org.apache.spark.metrics

import java.io.File
import java.nio.charset.{Charset, StandardCharsets}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.{DefaultCluster, SeparateJVM}
import com.datastax.spark.connector.embedded.SparkTemplate
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkContext, TaskContext}

class CassandraConnectorSourceSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with SeparateJVM {

  private def prepareConf = SparkTemplate.defaultConf.setMaster("local[*]")

  "CassandraConnectorSource" should "be initialized when it was specified in metrics properties" in {
    val className = classOf[CassandraConnectorSource].getName
    val metricsPropertiesContent =
      s"""
         |*.source.cassandra-connector.class=$className
       """.stripMargin

    val metricsPropertiesFile = File.createTempFile("spark-cassandra-connector", "metrics.properties")
    metricsPropertiesFile.deleteOnExit()
    FileUtils.writeStringToFile(metricsPropertiesFile, metricsPropertiesContent, StandardCharsets.UTF_8)

    val conf = prepareConf
    conf.set("spark.metrics.conf", metricsPropertiesFile.getAbsolutePath)
    val metricsSc = new SparkContext(conf)
    try {
      metricsSc.runJob(metricsSc.makeRDD(1 to 1), (tc: TaskContext, it: Iterator[Int]) => {
        MetricsUpdater.getSource(tc).toArray.length
      }).head shouldBe 1
    } finally {
      metricsSc.stop()
    }
  }

  it should "not be initialized when it wasn't specified in metrics properties" in {
    val metricsPropertiesContent =
      s"""
       """.stripMargin

    val metricsPropertiesFile = File.createTempFile("spark-cassandra-connector", "metrics.properties")
    metricsPropertiesFile.deleteOnExit()
    FileUtils.writeStringToFile(metricsPropertiesFile, metricsPropertiesContent, StandardCharsets.UTF_8)

    val conf = prepareConf
    val metricsSc = new SparkContext(conf)
    try {
      metricsSc.runJob(metricsSc.makeRDD(1 to 1), (tc: TaskContext, it: Iterator[Int]) => {
        MetricsUpdater.getSource(tc).toArray.length
      }).head shouldBe 0
    } finally {
      metricsSc.stop()
    }
  }

}
