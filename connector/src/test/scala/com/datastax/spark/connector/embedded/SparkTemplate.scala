package com.datastax.spark.connector.embedded

import java.nio.file.Files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object SparkTemplate {

  val DefaultParallelism = 2

  val HiveMetastoreConfig: Map[String, String] = Map (
    "spark.hadoop.hive.metastore.rawstore.impl" -> "com.datastax.bdp.hadoop.hive.metastore.CassandraHiveMetaStore",
    "spark.hadoop.cassandra.autoCreateHiveSchema" -> "true",
    "spark.hadoop.spark.enable" -> "true",
    "spark.hadoop.cassandra.connection.metaStoreColumnFamilyName" -> "sparkmetastore",
    "spark.sql.catalogImplementation" -> "hive",
    "spark.sql.warehouse.dir" -> Files.createTempDirectory("tempWarehouse").toString
  )


  /** Default configuration for [[org.apache.spark.SparkContext SparkContext]]. */
  private val _defaultConf = new SparkConf(true)
    .set("spark.cassandra.connection.keepAliveMS", "5000")
    .set("spark.cassandra.connection.timeoutMS", "30000")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")
    .set("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions")
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", s"local[$DefaultParallelism]"))
    .setAppName("Test")
    .setAll(HiveMetastoreConfig)


  def defaultConf = _defaultConf.clone()

  def withoutLogging[T]( f: => T): T={
    val level = Logger.getRootLogger.getLevel
    Logger.getRootLogger.setLevel(Level.OFF)
    val ret = f
    Logger.getRootLogger.setLevel(level)
    ret
  }

}
