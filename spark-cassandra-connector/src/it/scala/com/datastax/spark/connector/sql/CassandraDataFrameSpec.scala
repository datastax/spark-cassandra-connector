package com.datastax.spark.connector.sql

import java.io.IOException

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ProtocolVersion._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import com.datastax.spark.connector._
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import org.joda.time.LocalDate
import org.scalatest.concurrent.Eventually

case class RowWithV4Types(key: Int, a: Byte, b: Short, c: java.sql.Date)

class CassandraDataFrameSpec extends SparkCassandraITFlatSpecBase with Eventually{
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

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
      },

      Future {
        info ("Setting up Date Tables")
        skipIfProtocolVersionLT(V4) {
        session.execute(s"create table $ks.date_test (key int primary key, dd date)")
        session.execute(s"create table $ks.date_test2 (key int primary key, dd date)")
        session.execute(s"insert into $ks.date_test (key, dd) values (1, '1930-05-31')")
        }
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
          "keyspace" -> ks,
          "spark_cassandra_output_ttl" -> "300"
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

    val ttl = conn.withSessionDo { session =>
      val rs = session.execute(s"""SELECT TTL(v) from $ks.kv_copy""")
      rs.one().getInt(0)
    }

    ttl should be > 0
    ttl should be <= 300
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

  it should " provide useful messages when creating a table with columnName mismatches" in {
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

    val pkError = intercept[IllegalArgumentException] {
      df.createCassandraTable(ks, "kv_auto", Some(Seq("cara")))
    }
    pkError.getMessage should include ("\"cara\" not Found.")

    val ccError = intercept[IllegalArgumentException] {
      df.createCassandraTable(ks, "kv_auto", Some(Seq("k")), Some(Seq("sundance")))
    }
    ccError.getMessage should include ("\"sundance\" not Found.")

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

  it should "read and write C* LocalDate columns" in skipIfProtocolVersionLT(V4){
    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "date_test", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .load

    df.count should be (1)
    df.first.getDate(1) should be (new LocalDate(1930, 5, 31).toDate)

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "date_test2", "keyspace" -> ks, "cluster" -> "ClusterOne"))
      .save

    conn.withSessionDo { session =>
      session.execute(s"select count(1) from $ks.date_test2").one().getLong(0) should be (1)
    }
  }

  it should "be able to write to ProtocolVersion 3 Tables correctly with V4 Types" in skipIfProtocolVersionGTE(V4){

    val table = "newtypetable"

    val rdd = sc.parallelize(1 to 100).map( x =>
      RowWithV4Types(x, Byte.MinValue, Short.MinValue, java.sql.Date.valueOf("2016-08-03")))

    val df = sqlContext.createDataFrame(rdd)
    df.createCassandraTable(ks, table)

    val tableColumns = eventually(
      conn.withClusterDo(_.getMetadata.getKeyspace(ks).getTable(table)).getColumns.map(_.getType))

    tableColumns should contain theSameElementsInOrderAs(
      Seq(DataType.cint(), DataType.cint(), DataType.cint(), DataType.timestamp()))

    df.write.cassandraFormat(table, ks).save()

    val  rows = sqlContext
      .read
      .cassandraFormat(table, ks)
      .load()
      .collect
      .map(row => (row.getInt(1), row.getInt(2), row.getTimestamp(3).toString))

    val firstRow = rows(0)
    firstRow should be((Byte.MinValue.toInt, Short.MinValue.toInt, "2016-08-03 00:00:00.0"))
  }

  it should "be able to set splitCount" in {
    val df = sqlContext
      .read
      .cassandraFormat("kv", ks)
      .option("splitCount", "120")
      .load

    val rdd = df.rdd.dependencies.head.rdd.dependencies.head.rdd.asInstanceOf[CassandraTableScanRDD[_]]
    rdd.readConf.splitCount should be (Some(120))
  }


}
