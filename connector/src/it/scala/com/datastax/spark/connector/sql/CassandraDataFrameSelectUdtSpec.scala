package com.datastax.spark.connector.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually

class CassandraDataFrameSelectUdtSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually with Matchers {
  override lazy val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)

    session.execute(
      s"""CREATE TYPE ${ks}.embedded(
        |        a TEXT,
        |        b INT
        |    )""".stripMargin
    )

    session.execute(
      s"""CREATE TABLE ${ks}.crash_test(
        |        id INT,
        |        embeddeds LIST<FROZEN<embedded>>,
        |        single embedded,
        |        embedded_map MAP<INT, FROZEN<embedded>>,
        |        embedded_set SET<FROZEN<embedded>>,
        |        PRIMARY KEY (id)
        |    )""".stripMargin
    )

    session.execute(
      s"""INSERT INTO ${ks}.crash_test JSON '{"id": 1, "embeddeds": [], "embedded_map": {}, "embedded_set": []}'"""
    )
    session.execute(
      s"""INSERT INTO ${ks}.crash_test JSON
         |'{
         |  "id": 2,
         |  "single": {"a": "a1", "b": 1},
         |  "embeddeds": [{"a": "x1", "b": 1}, {"a": "x2", "b": 2}],
         |  "embedded_map": {"1": {"a": "x1", "b": 1}, "2": {"a": "x2", "b": 2}},
         |  "embedded_set": [{"a": "x1", "b": 1}, {"a": "x2", "b": 2}]
         |}'
         |""".stripMargin
    )
  }

  trait Env {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "crash_test",
          "keyspace" -> ks
        )
      )
      .load()
      // .cache() // this works
  }

  it should "allow selecting single elements" in new Env {
    val elements = df.select(col("id"), col("single.b")).collect().map { row =>
      optionalInt(row, 1).getOrElse(0)
    }
    elements should contain theSameElementsAs Seq(0, 1)
  }

  it should "allow selecting single elements with renaming" in new Env {
    df.select(col("single.b").as("x")).collect().map { row =>
      optionalInt(row, 0).getOrElse(0)
    } should contain theSameElementsAs Seq(0,1)
  }

  it should "allow selecting projections in lists" in new Env {
    val elements = df.select(col("id"), col("embeddeds.b")).collect().map { row =>
      row.getAs[Seq[Int]](1).sum
    }
    elements should contain theSameElementsAs Seq(0, 3)
  }

  it should "allow selecting projections in maps" in new Env {
    val elements = df.select("embedded_map.1.b").collect().map { row =>
      optionalInt(row, 0).getOrElse(0)
    }
    elements should contain theSameElementsAs Seq(0, 1)
  }

  it should "allow selecting projections in sets" in new Env {
    val elements = df.select("embedded_set.b").collect().map { row =>
      row.getAs[Seq[Int]](0).sum
    }
    elements should contain theSameElementsAs Seq(0, 3)
  }

  private def optionalInt(row: Row, idx: Int): Option[Int] = {
    if (row.isNullAt(idx)) {
      None
    } else {
      Some(row.getInt(idx))
    }
  }
}
