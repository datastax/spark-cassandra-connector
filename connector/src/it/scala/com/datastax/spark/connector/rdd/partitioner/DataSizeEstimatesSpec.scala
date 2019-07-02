package com.datastax.spark.connector.rdd.partitioner

import scala.language.postfixOps
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.dht.LongToken

class DataSizeEstimatesSpec extends SparkCassandraITFlatSpecBase {
  override val conn = CassandraConnector(defaultConf)

  val tableName = "table1"


  override def beforeClass(): Unit = {
    conn.withSessionDo { session => createKeyspace(session) }


    conn.withSessionDo { session =>
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
      val futures = for (i <- 1 to 1000) yield
        session.executeAsync(s"INSERT INTO $ks.$tableName(key, value) VALUES (?, ?)",
          i.asInstanceOf[AnyRef],
          "value" + i)
      futures.par.foreach(_.getUninterruptibly)
    }

    //Force a flush by restarting the node
    ccmBridge.stop(1)
    ccmBridge.start(1)
    ccmBridge.waitForUp(1)
  }

  "DataSizeEstimates" should "fetch data size estimates for a known table" in {
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
