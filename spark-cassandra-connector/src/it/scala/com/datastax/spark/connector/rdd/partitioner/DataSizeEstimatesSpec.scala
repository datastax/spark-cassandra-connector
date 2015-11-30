package com.datastax.spark.connector.rdd.partitioner

import scala.language.postfixOps

import org.scalatest.{Matchers, FlatSpec}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{CassandraRunner, EmbeddedCassandra}
import com.datastax.spark.connector.rdd.partitioner.dht.LongToken
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra

class DataSizeEstimatesSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)
  }

  "DataSizeEstimates" should "fetch data size estimates for a known table" in {
    val tableName = "table1"
    conn.withSessionDo { session =>
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
      val futures = for (i <- 1 to 1000) yield
        session.executeAsync(s"INSERT INTO $ks.$tableName(key, value) VALUES (?, ?)",
          i.asInstanceOf[AnyRef],
          "value" + i)
      futures.par.foreach(_.getUninterruptibly)
    }

    import scala.concurrent.duration._
    for (runner <- EmbeddedCassandra.cassandraRunners(0)) {
      runner.nodeToolCmd("flush")
      val initialDelay =
        Math.max(runner.startupTime + (45 seconds).toMillis - System.currentTimeMillis(), 0L)
      Thread.sleep(
        initialDelay + 2L * (CassandraRunner.SizeEstimatesUpdateIntervalInSeconds seconds).toMillis)
    }

    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount should be > 500L
    estimates.partitionCount should be < 2000L
    estimates.dataSizeInBytes should be > 0L
  }

  it should "should return zeroes for an empty table" in {
    val tableName = "table2"
    conn.withSessionDo { session =>
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
    }

    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }

  it should "return zeroes for a non-existing table" in {
    val tableName = "table3"
    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }
}
