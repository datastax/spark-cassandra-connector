package org.apache.spark.sql

import java.nio.file.Files

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future

class CassandraStreamingSinkSpec extends SparkCassandraITFlatSpecBase with Logging with Eventually{

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.kv (key int, value int, PRIMARY KEY (key))""".stripMargin)
      },
      Future {
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.empty (key int, value int, PRIMARY KEY (key))""".stripMargin)
      }

    )
  }

  "CassandraStreamingSink" should " be able to written to from a stream" in {

    val checkpointDir = Files.createTempDirectory("ks")

    val source = sparkSession
      .readStream
      .format("com.datastax.spark.connector.test.monotonic")
      .load()
      .withColumn("value", col("key") + 1)
      .withColumn("key", col("key")) // SparkStreaming seems to rename "key" if we don't do this

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("kv", ks)
      .outputMode(OutputMode.Update)
      .start()

    eventually (timeout(Span(30, Seconds))) {
      val lastBatch = query.lastProgress.batchId
      if (query.exception.nonEmpty) {
        println(query.explain)
        println(query.exception)
      }
      lastBatch should be > 2L
    }

    query.stop()

    val rs = conn.withSessionDo( s => s.execute(s"SELECT Count(*) FROM $ks.kv").all() )
    rs.get(0).getLong(0) should be > 200L
  }


  it should " be able to written to from an empty stream" in {

    val checkpointDir = Files.createTempDirectory("ks")

    val source = sparkSession
      .readStream
      .format("com.datastax.spark.connector.test.empty")
      .load()
      .withColumn("value", col("key") + 1)
      .withColumn("key", col("key")) // SparkStreaming seems to rename "key" if we don't do this

    val query = source.writeStream
      .option("checkpointLocation", checkpointDir.toString)
      .cassandraFormat("empty", ks)
      .outputMode(OutputMode.Update)
      .start()

    eventually (timeout(Span(30, Seconds))) {
      if (query.exception.nonEmpty) {
        println(query.explain)
        println(query.exception)
      }
      val lastBatch = query.lastProgress.batchId
      lastBatch should be > 2L
    }

    query.stop()

    val rs = conn.withSessionDo( s => s.execute(s"SELECT Count(*) FROM $ks.empty").all() )
    rs.get(0).getLong(0) should be (0L)
  }
}
