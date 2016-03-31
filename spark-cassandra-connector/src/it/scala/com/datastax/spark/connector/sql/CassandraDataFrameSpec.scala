package com.datastax.spark.connector.sql

import java.io.IOException

import scala.collection.JavaConversions._
import scala.concurrent.Future

import com.datastax.spark.connector._
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SQLContext

class CassandraDataFrameSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf)

  val conn = CassandraConnector(defaultConf)

  val sqlContext: SQLContext = new SQLContext(sc)

  def pushDown: Boolean = true

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks.kv_copy (k INT, v TEXT, PRIMARY KEY (k))
             |""".stripMargin)
      },

      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks.hardtoremembernamedtable (k INT, v TEXT, PRIMARY KEY (k))
             |""".stripMargin)
      },

      Future {
        session.execute(
          s"""
              |CREATE TABLE IF NOT EXISTS $ks.kv (k INT, v TEXT, PRIMARY KEY (k))
              |""".stripMargin)

        val prepared = session.prepare( s"""INSERT INTO $ks.kv (k, v) VALUES (?, ?)""")

        (for (x <- 1 to 1000) yield {
          session.executeAsync(prepared.bind(x: java.lang.Integer, x.toString))
        }).par.foreach(_.getUninterruptibly)
      },

      Future {
        session.execute(s"CREATE TABLE $ks.tuple_test1 (id int, t Tuple<text, int>, PRIMARY KEY (id))")
        session.execute(s"CREATE TABLE $ks.tuple_test2 (id int, t Tuple<text, int>, PRIMARY KEY (id))")
        session.execute(s"INSERT INTO $ks.tuple_test1 (id, t) VALUES (1, ('xyz', 3))")
      }
    )
  }

  "A DataFrame" should "be able to be created programmatically" in {
    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.count() should be(1000)
  }

  it should "be able to be saved programmatically" in {
    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv_copy",
          "keyspace" -> ks
        )
      )
      .save()

    val dfCopy = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv_copy",
          "keyspace" -> ks
        )
      )
      .load()

    dfCopy.count() should be (1000)
  }

  it should " be able to create a C* schema from a table" in {
     val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "kv",
          "keyspace" -> ks
        )
      )
      .load()

    df.createCassandraTable(ks, "kv_auto", Some(Seq("v")), Some(Seq("k")))

    val meta = conn.withClusterDo(_.getMetadata)
    val autoTableMeta = meta.getKeyspace(ks).getTable("kv_auto")
    autoTableMeta.getPartitionKey.map(_.getName) should contain ("v")
    autoTableMeta.getClusteringColumns.map(_.getName) should contain ("k")

  }

  it should " provide error out with a sensible message when a table can't be found" in {
    val exception = intercept[IOException] {
      val df = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(
          Map(
            "table" -> "randomtable",
            "keyspace" -> ks
          )
        )
        .load()
    }
    exception.getMessage should include("Couldn't find")
  }

  it should " provide useful suggestions if a table can't be found but a close match exists" in {
    val exception = intercept[IOException] {
      val df = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(
          Map(
            "table" -> "hardertoremembertablename",
            "keyspace" -> ks
          )
        )
        .load
    }
    exception.getMessage should include("Couldn't find")
    exception.getMessage should include("hardtoremembernamedtable")
  }

  it should "read and write C* Tuple columns" in {
    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tuple_test1", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .load

    df.count should be (1)
    df.first.getStruct(1).getString(0) should be ("xyz")
    df.first.getStruct(1).getInt(1) should be (3)

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tuple_test2", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .save

    conn.withSessionDo { session =>
      session.execute(s"select count(1) from $ks.tuple_test2").one().getLong(0) should be (1)
    }
  }


}