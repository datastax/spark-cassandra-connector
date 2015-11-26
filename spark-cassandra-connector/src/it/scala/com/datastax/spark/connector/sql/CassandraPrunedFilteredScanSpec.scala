package com.datastax.spark.connector.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

class CassandraPrunedFilteredScanSpec extends SparkCassandraITFlatSpecBase with Logging  {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)
  val conn = CassandraConnector(defaultConf)
  val sqlContext: SQLContext = new SQLContext(sc)

  val keyspace = "pss_spec"
  val cassandraFormat = "org.apache.spark.sql.cassandra"

  override def beforeAll(): Unit = {
    conn.withSessionDo { session =>
      session.execute( s"""DROP KEYSPACE IF EXISTS "$keyspace"""")
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS "$keyspace" WITH REPLICATION =
            |{ 'class': 'SimpleStrategy',
            |'replication_factor': 1 }""".stripMargin)

      session.execute(
        s"""CREATE TABLE IF NOT EXISTS "$keyspace".colors
            |(name TEXT, color TEXT, priority INT, PRIMARY KEY (name, priority)) """
          .stripMargin)

      session.execute(
        s"""CREATE TABLE IF NOT EXISTS "$keyspace".fields
            |(k INT, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, PRIMARY KEY (k)) """
          .stripMargin)
    }
  }

  val colorOptions = Map("keyspace" -> keyspace, "table" -> "colors")
  val fieldsOptions = Map("keyspace" -> keyspace, "table" -> "fields")
  val withPushdown = Map("pushdown" -> "true")
  val withoutPushdown = Map("pushdown" -> "false")


  /** The internals of which predicates are actual filtered are hidden within the SparkPlan internals
    * so we'll need to test with string matching
    */

  "CassandraPrunedFilteredScan" should "pushdown predicates for clustering keys" in {
    val colorDF = sqlContext.read.format(cassandraFormat).options(colorOptions ++ withPushdown).load()
    val executionPlan = colorDF.filter("priority > 5").queryExecution.executedPlan.toString
    executionPlan should include ("PushedFilter: [GreaterThan(priority,5)]")
  }

  it should "not pushdown predicates for clustering keys if filterPushdown is disabled" in {
    val colorDF = sqlContext.read.format(cassandraFormat).options(colorOptions ++ withoutPushdown).load()
    val executionPlan = colorDF.filter("priority > 5").queryExecution.executedPlan.toString
    executionPlan should include regex """Filter \(priority#\d+ > 5\)""".r
  }

  it should "prune data columns" in {
    val fieldsDF = sqlContext.read.format(cassandraFormat).options(fieldsOptions ++ withPushdown).load()
    val executionPlan = fieldsDF.select("b","c","d").queryExecution.executedPlan.toString
    executionPlan should include regex """PushedFilter: \[\] \[b#\d+,c#\d+,d#\d+\]""".r
  }

  it should "prune data columns if filterPushdown is disabled" in {
    val fieldsDF = sqlContext.read.format(cassandraFormat).options(fieldsOptions ++ withoutPushdown).load()
    val executionPlan = fieldsDF.select("b","c","d").queryExecution.executedPlan.toString
    executionPlan should include regex """PushedFilter: \[\] \[b#\d+,c#\d+,d#\d+\]""".r
  }

}