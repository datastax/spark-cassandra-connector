package com.datastax.spark.connector.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
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
        |        PRIMARY KEY (id)
        |    )""".stripMargin
    )

    session.execute(
      s"""INSERT INTO ${ks}.crash_test JSON '{"id": 1, "embeddeds": []}'"""
    )
    session.execute(
      s"""INSERT INTO ${ks}.crash_test JSON '{"id": 1, "embeddeds": [{"a": "x1", "b": 1}, {"a": "x2", "b": 2}]}'"""
    )
  }


  it should "allow selecting projections" in {
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

    val elements = df.select(col("embeddeds.b")).collect().flatMap { row =>
      if (row.isNullAt(0)) {
        None
      } else {
        Some(row.getInt(0))
      }
    }
    elements shouldBe Seq(2)
  }

}
