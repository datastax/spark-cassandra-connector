package org.apache.spark.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.CassandraCatalog
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.CatalystUtil._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.internal.SQLConf

import scala.concurrent.Future

class CassandraSourceOptionsSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

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
    val df = spark
      .read
      .cassandraFormat("colors", ks).option(ReadConf.ReadsPerSecParam.name.toUpperCase(), "9001")
      .load()

    val scan = findCassandraScan(df.queryExecution.sparkPlan).get

    scan.readConf.readsPerSec.get should be (9001)
  }

  it should "be configurable in Spark SQL" in {

    spark.conf.set(s"spark.sql.catalog.cassandra", classOf[CassandraCatalog].getCanonicalName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "cassandra")
    spark.sql("SET spark.cassandra.input.readsPerSec=9001")
    val df = spark.sql(s"SELECT * FROM $ks.colors")
    val scan = findCassandraScan(df.queryExecution.sparkPlan).get
    scan.readConf.readsPerSec.get should be (9001)
  }

}
