package org.apache.spark.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.util.CatalystUtil._
import org.apache.spark.sql.cassandra._

import scala.concurrent.Future

class CassandraSourceOptionsSpec  extends SparkCassandraITFlatSpecBase with DefaultCluster with Logging  {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.colors
               |(name TEXT, color TEXT, priority INT, PRIMARY KEY (name, priority)) """
              .stripMargin)
        }
      )
    }
  }

  "Source options" should "be case insensitive" in {
    val df = sparkSession
      .read
      .cassandraFormat("colors", ks).option(ReadConf.ReadsPerSecParam.name.toUpperCase(), "9001")
      .load()

    val rdd = findCassandraTableScanRDD(df.queryExecution.sparkPlan).get

    rdd.readConf.readsPerSec.get should be (9001)
  }

  it should "be readable in SparkSQL case insensitive" in {
    sparkSession.sql(
      s"""CREATE TABLE colors USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | TaBLe "colors",
        | KEYSpace "$ks",
        | sParK.CassandrA.inPUT.REAdsPERsec "9001"
        | )
      """.stripMargin)
    val df = sparkSession.sql("SELECT * FROM colors")

    val rdd = findCassandraTableScanRDD(df.queryExecution.sparkPlan).get

    rdd.readConf.readsPerSec.get should be (9001)
  }

}
