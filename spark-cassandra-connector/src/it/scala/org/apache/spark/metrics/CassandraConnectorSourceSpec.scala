package org.apache.spark.metrics

import java.io.File

import com.datastax.spark.connector.embedded.SparkTemplate
import org.apache.commons.io.FileUtils
import org.apache.spark.{TaskContext, SparkEnv, SparkConf}
import org.scalatest.{FlatSpec, Matchers}

class CassandraConnectorSourceSpec extends FlatSpec with Matchers with SparkTemplate {

  def prepareConf = {
    val conf = new SparkConf(loadDefaults = false)
    conf.setMaster("local[*]")
    conf.setAppName("test")
    conf
  }

  "CassandraConnectorSource" should "be initialized when it was specified in metrics properties" in {
    val className = classOf[CassandraConnectorSource].getName
    val metricsPropertiesContent =
      s"""
         |*.source.cassandra-connector.class=$className
       """.stripMargin

    val metricsPropertiesFile = File.createTempFile("spark-cassandra-connector", "metrics.properties")
    metricsPropertiesFile.deleteOnExit()
    FileUtils.writeStringToFile(metricsPropertiesFile, metricsPropertiesContent)

    val conf = prepareConf
    conf.set("spark.metrics.conf", metricsPropertiesFile.getAbsolutePath)
    useSparkConf(conf)
    try {
      sc.runJob(sc.makeRDD(1 to 1), (tc: TaskContext, it: Iterator[Int]) => {
        MetricsUpdater.getSource(tc).toArray.length
      }).head shouldBe 1
    } finally {
      sc.stop()
    }
  }

  it should "not be initialized when it wasn't specified in metrics properties" in {
    val metricsPropertiesContent =
      s"""
       """.stripMargin

    val metricsPropertiesFile = File.createTempFile("spark-cassandra-connector", "metrics.properties")
    metricsPropertiesFile.deleteOnExit()
    FileUtils.writeStringToFile(metricsPropertiesFile, metricsPropertiesContent)

    val conf = prepareConf
    conf.set("spark.metrics.conf", metricsPropertiesFile.getAbsolutePath)

    useSparkConf(conf)
    try {
      sc.runJob(sc.makeRDD(1 to 1), (tc: TaskContext, it: Iterator[Int]) => {
        MetricsUpdater.getSource(tc).toArray.length
      }).head shouldBe 0
    } finally {
      sc.stop()
    }
  }

}
