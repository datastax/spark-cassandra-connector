package com.datastax.spark.connector.sql

import java.io.IOException

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.apache.spark.sql.SQLContext

class CassandraDataFrameSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  val keyspace = "DataFramz"


  val sqlContext: SQLContext = new SQLContext(sc)

  def pushDown: Boolean = true

  override def beforeAll(): Unit = {
    conn.withSessionDo { session =>
      session.execute( s"""DROP KEYSPACE IF EXISTS "$keyspace"""")
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS "$keyspace" WITH REPLICATION =
                                                      |{ 'class': 'SimpleStrategy',
                                                      |'replication_factor': 1 }""".stripMargin)

      session.execute(
        s"""CREATE TABLE IF NOT EXISTS "$keyspace".kv
                                                   |(k INT, v TEXT, PRIMARY KEY (k)) """
          .stripMargin)

      session.execute(
        s"""CREATE TABLE IF NOT EXISTS "$keyspace".hardtoremembernamedtable
                                                   |(k INT, v TEXT, PRIMARY KEY (k)) """
          .stripMargin)

      val prepared = session.prepare( s"""INSERT INTO "$keyspace".kv (k,v) VALUES (?,?)""")

      for (x <- 1 to 1000) {
        session.execute(prepared.bind(x: java.lang.Integer, x.toString))
      }
    }
  }

  override def afterAll() {
    super.afterAll()
    conn.withSessionDo { session =>
      session.execute( s"""DROP KEYSPACE IF EXISTS "$keyspace"""")
    }
  }

  "A DataFrame" should "be able to be created programmatically" in {
    val df = sqlContext.load(
      "org.apache.spark.sql.cassandra",
      Map(
        "c_Table" -> "kv",
        "keyspace" -> keyspace
      )
    )
    df.count() should be(1000)
  }

  it should " provide error out with a sensible message when a table can't be found" in {
    val exception = intercept[IOException] {
      val df = sqlContext.load(
        "org.apache.spark.sql.cassandra",
        Map(
          "c_Table" -> "randomtable",
          "keyspace" -> keyspace
        )
      )
    }
    exception.getMessage should include("Couldn't find")
  }

  it should " provide useful suggestions if a table can't be found but a close match exists" in {
    val exception = intercept[IOException] {
      val df = sqlContext.load(
        "org.apache.spark.sql.cassandra",
        Map(
          "c_Table" -> "hardertoremembertablename",
          "keyspace" -> keyspace
        )
      )
    }
    exception.getMessage should include("Couldn't find")
    exception.getMessage should include("hardtoremembernamedtable")
  }
}
