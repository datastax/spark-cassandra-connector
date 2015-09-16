package com.datastax.spark.connector.rdd.partitioner

import scala.language.postfixOps

import org.scalatest.{Matchers, FlatSpec}

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{CassandraRunner, EmbeddedCassandra}
import com.datastax.spark.connector.rdd.partitioner.dht.LongToken
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra

class DataSizeEstimatesSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(hosts = Set(EmbeddedCassandra.getHost(0)))

  val keyspaceName = "data_size_estimates"

  conn.withSessionDo { session =>
    session.execute(
      s"CREATE KEYSPACE IF NOT EXISTS $keyspaceName " +
        s"WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
  }

  // TODO: enable this test once we upgrade to 2.1.5, which populates the size estimates table
  // We cannot enable this test till we have a way of forcing the size estimates table to update
  // https://issues.apache.org/jira/browse/CASSANDRA-9579 should provide us with the hook neccessary
  // to trigger this event
  "DataSizeEstimates" should "fetch data size estimates for a known table" in {
    val tableName = "table1"
    conn.withSessionDo { session =>
      session.execute(
        s"CREATE TABLE IF NOT EXISTS $keyspaceName.$tableName(key int PRIMARY KEY, value VARCHAR)")
      for (i <- 1 to 10000)
        session.execute(
          s"INSERT INTO $keyspaceName.$tableName(key, value) VALUES (?, ?)",
          i.asInstanceOf[AnyRef],
          "value" + i)
    }

    import scala.concurrent.duration._
    for (runner <- EmbeddedCassandra.cassandraRunners(0)) {
      runner.nodeToolCmd("flush")
      val initialDelay =
        Math.max(runner.startupTime + (45 seconds).toMillis - System.currentTimeMillis(), 0L)
      Thread.sleep(
        initialDelay + 2L * (CassandraRunner.SizeEstimatesUpdateIntervalInSeconds seconds).toMillis)
    }

    val estimates = new DataSizeEstimates[Long, LongToken](conn, keyspaceName, tableName)
    estimates.partitionCount should be > 5000L
    estimates.partitionCount should be < 20000L
    estimates.dataSizeInBytes should be > 0L
  }

  it should "should return zeroes for an empty table" in {
    val tableName = "table2"
    conn.withSessionDo { session =>
      session.execute(
        s"CREATE TABLE IF NOT EXISTS $keyspaceName.$tableName(key int PRIMARY KEY, value VARCHAR)")
    }

    val estimates = new DataSizeEstimates[Long, LongToken](conn, keyspaceName, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }

  it should "return zeroes for a non-existing table" in {
    val tableName = "table3"
    val estimates = new DataSizeEstimates[Long, LongToken](conn, keyspaceName, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }
}
