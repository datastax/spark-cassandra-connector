package com.datastax.spark.connector.embedded

import java.io.File

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

import scala.util.Try

trait SparkTemplate {
  /** Obtains the active [[org.apache.spark.SparkContext SparkContext]] object. */
  def sc: SparkContext = SparkTemplate.sc

  /** Obtains the active [[org.apache.spark.sql.SparkSession SparkSession]] object. */
  def sparkSession: SparkSession = SparkTemplate.sparkSession

  /** Ensures that the currently running [[org.apache.spark.SparkContext SparkContext]] uses the provided
    * configuration. If the configurations are different or force is `true` Spark context is stopped and
    * started again with the given configuration. */
  def useSparkConf(conf: SparkConf, force: Boolean = false): SparkSession =
    SparkTemplate.useSparkConf(conf, force)

  def defaultConf = SparkTemplate.defaultConf

  /**
    * Creates CassandraHiveMetastore and returns SparkConf to connect to it
    */
  def metastoreConf:SparkConf = {
    CassandraConnector(defaultConf).withSessionDo { session =>
      session.execute(
        """
          |CREATE KEYSPACE IF NOT EXISTS "HiveMetaStore" WITH REPLICATION =
          |{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }; """
          .stripMargin)
      session.execute(
        """CREATE TABLE IF NOT EXISTS "HiveMetaStore"."sparkmetastore"
          |(key text,
          |entity text,
          |value blob,
          |PRIMARY KEY (key, entity))""".stripMargin)
    }
    defaultConf.setAll(SparkTemplate.HiveMetastoreConfig)
  }
}

object SparkTemplate {

  /** Default configuration for [[org.apache.spark.SparkContext SparkContext]]. */
  val _defaultConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", getHost(0).getHostAddress)
    .set("spark.cassandra.connection.port", getPort(0).toString)
    .set("spark.cassandra.connection.keepAliveMS", "5000")
    .set("spark.cassandra.connection.timeoutMS", "30000")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", "local[2]"))
    .setAppName("Test")

  val HiveMetastoreConfig: Map[String, String] = Map (
    "spark.hadoop.hive.metastore.rawstore.impl" -> "com.datastax.bdp.hadoop.hive.metastore.CassandraHiveMetaStore",
    "spark.hadoop.cassandra.autoCreateHiveSchema" -> "true",
    "spark.hadoop.spark.enable" -> "true",
    "spark.hadoop.cassandra.connection.metaStoreColumnFamilyName" -> "sparkmetastore",
    "spark.sql.catalogImplementation" -> "hive"
  )

  def defaultConf = _defaultConf.clone()

  private var _session: SparkSession = _

  private var _conf: SparkConf = _
  /** Ensures that the currently running [[org.apache.spark.SparkContext SparkContext]] uses the provided
    * configuration. If the configurations are different or force is `true` Spark context is stopped and
    * started again with the given configuration. */
  def useSparkConf(conf: SparkConf = SparkTemplate.defaultConf, force: Boolean = false): SparkSession = {
    if (conf == null) {
      _conf = null
      if (_session != null) {
        _session.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        _session = null
      }
    } else if (force || _conf == null || _conf.getAll.toMap != conf.getAll.toMap)
      resetSparkContext(conf)
    _session
  }

  private def resetSparkContext(conf: SparkConf) = {
    if (_session != null) {
      _session.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

    System.err.println("Starting SparkContext with the following configuration:\n" +
      conf.toDebugString.lines.map(line => "\t" + line).mkString("\n"))
    _conf = conf.clone()
    for (cp <- sys.env.get("SPARK_SUBMIT_CLASSPATH"))
      conf.setJars(
        cp.split(File.pathSeparatorChar)
          .filter(_.endsWith(".jar"))
          .map(new File(_).getAbsoluteFile.toURI.toString))
    _session = SparkSession.builder().config(conf).getOrCreate()
    _session
  }

  /** Obtains the active [[org.apache.spark.SparkContext SparkContext]] object. */
  def sc: SparkContext = _session.sparkContext
  def sparkSession: SparkSession = _session

  def withoutLogging[T]( f: => T): T={
    val level = Logger.getRootLogger.getLevel
    Logger.getRootLogger.setLevel(Level.OFF)
    val ret = f
    Logger.getRootLogger.setLevel(level)
    ret
  }

}
