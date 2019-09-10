package com.datastax.spark.connector.sql

import java.util.concurrent.CompletableFuture

import com.datastax.oss.driver.api.core.Version

import scala.concurrent.Future
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.types.UserDefinedType
import com.datastax.spark.connector.util.schemaFromCassandra
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually

class CassandraDataFrameMetadataSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually with Matchers {
  override lazy val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks.basic (k INT, c INT, v INT, v2 INT, PRIMARY KEY (k,c))
             |""".stripMargin)

        val prepared = session.prepare(
          s"""INSERT INTO $ks.basic (k, c, v, v2) VALUES (?, ?, ?, ?)
             |USING TTL ? AND TIMESTAMP ?""".stripMargin)

        val results = (for (x <- 1 to 100) yield {
          session.executeAsync(prepared.bind(
            x: java.lang.Integer,
            x: java.lang.Integer,
            x: java.lang.Integer,
            x: java.lang.Integer,
            ((x * 10000): java.lang.Integer),
            x.toLong: java.lang.Long)).toCompletableFuture
        })
        CompletableFuture.allOf(results: _*).get
      },
      Future {
        session.execute(
          s"""
             |CREATE TYPE $ks.fullname (
             |    firstname text,
             |    lastname text
             |)
         """.stripMargin)
        session.execute(
          s"""
             |CREATE TABLE $ks.test_reading_types (
             |    id bigint PRIMARY KEY,
             |    list_val list<int>,
             |    list_val_frozen frozen<list<int>>,
             |    map_val map<text, int>,
             |    map_val_frozen frozen<map<text, int>>,
             |    set_val set<int>,
             |    set_val_frozen frozen<set<int>>,
             |    simple_val int,
             |    udt_frozen_val frozen<fullname>,
             |    udt_val fullname,
             |    tuple_val tuple <int, int>,
             |    tuple_val_frozen frozen<tuple<int, int>>
             |)
        """.stripMargin)
        session.execute(
          s"""
             |insert into $ks.test_reading_types (id, simple_val, list_val, list_val_frozen,
             |map_val, map_val_frozen, set_val, set_val_frozen, udt_val, udt_frozen_val, tuple_val,
             |tuple_val_frozen) values
             |(0, 1,
             |[2, 3], [2, 3],
             |{'four': 4, 'five': 5}, {'four': 4, 'five': 5},
             |{6, 7}, {6, 7},
             |{firstname: 'Joe', lastname: 'Smith'}, {firstname: 'Bredo', lastname: 'Morstoel'},
             |(1, 1), (1, 1)) USING
             |timestamp 1000
           """.stripMargin)
      },
      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks."caseNames" ("Key" INT, "Value" INT, "Dot.Value" INT, PRIMARY KEY ("Key"))
             |""".stripMargin)

        session.execute(
          s"""INSERT INTO $ks."caseNames" ("Key", "Value", "Dot.Value") VALUES (1, 2, 3)
             |USING TTL 10000 AND TIMESTAMP 10000""".stripMargin)
      }
    )
  }

  //Register Functions for TTL (Required for Spark < 3.0 or non DSE Spark 2.4)
  sparkSession.sessionState.functionRegistry.registerFunction(FunctionIdentifier("ttl"), CassandraMetadataFunction.cassandraTTLFunctionBuilder)
  sparkSession.sessionState.functionRegistry.registerFunction(FunctionIdentifier("writetime"), CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder)

  val dseVersion = cluster.getDseVersion.getOrElse(Version.parse("6.0.0"))

  val columnsToCheck = schemaFromCassandra(conn, Some(ks), Some("test_reading_types"))
    .tables
    .head
    .regularColumns
    .filter( columnDef =>
      if (dseVersion.getMajor >= 6 && dseVersion.getMinor >= 7) {
        //TODO: CHANGE THIS TO TRUE after : https://datastax-oss.atlassian.net/browse/JAVA-2371
        (!(columnDef.isCollection || columnDef.columnType.isInstanceOf[UserDefinedType]))
      }
      else {
        (!(columnDef.isCollection || columnDef.columnType.isInstanceOf[UserDefinedType]))
      })

  "A DataFrame" should "be able to read TTL" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks,
          "ttl.v" -> "ttlOfV"
        )
      )
      .load()

    val result = df.select(sum("ttlOfV").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "handle various attribute orderings" in {
    val df = sparkSession
      .read
      .cassandraFormat("test_reading_types", ks)
      .option("ttl.simple_val", "simple_val_TTL").load()

    val a = df.select("simple_val_TTL", "id", "map_val").collect.head.toSeq
    val b = df.select("id", "map_val", "simple_val_TTL").collect.head.toSeq
    val c = df.select("map_val", "simple_val_TTL", "id").collect.head.toSeq

    a should contain theSameElementsAs (b)
    b should contain theSameElementsAs (c)
  }

  for (col <- columnsToCheck) {
    val name = col.columnName
    it should s" handle ttl on $name ${col.columnType} with option" in {
      val df = sparkSession
        .read
        .cassandraFormat("test_reading_types", ks)
        .option(s"ttl.${col.columnName}", "ttlResult")
        .load()

      val result = df.collect().head
      if (col.isMultiCell) {
        result.getList[Int](result.fieldIndex("ttlResult")) should contain theSameElementsAs Seq(null, null)
      } else {
        result.get(result.fieldIndex("ttlResult")).asInstanceOf[AnyRef] should be (null)
      }
    }

    it should s" handle ttl on $name ${col.columnType} with function" in {
      val df = sparkSession
        .read
        .cassandraFormat("test_reading_types", ks)
        .load()
        .select(ttl(col.columnName).as("ttlResult"))

      val result = df.collect().head
      if (col.isMultiCell) {
        result.getList[Int](result.fieldIndex("ttlResult")) should contain theSameElementsAs Seq(null, null)
      } else {
        result.get(result.fieldIndex("ttlResult")).asInstanceOf[AnyRef] should be (null)
      }
    }

    it should s" handle writeTime on $name ${col.columnType} with option" in {
      val df = sparkSession
        .read
        .cassandraFormat("test_reading_types", ks)
        .option(s"writeTime.${col.columnName}", "writeTimeResult")
        .load()

      val result = df.collect().head
      if (col.isMultiCell) {
        result.getList[Long](result.fieldIndex("writeTimeResult")) should contain theSameElementsAs Seq(1000, 1000)
      } else {
        result.get(result.fieldIndex("writeTimeResult")).asInstanceOf[AnyRef] should be (1000)
      }
    }

    it should s" handle writeTime on $name ${col.columnType} with function" in {
      val df = sparkSession
        .read
        .cassandraFormat("test_reading_types", ks)
        .load()
        .select(writeTime(col.columnName).as("writeTimeResult"))

      val result = df.collect().head
      if (col.isMultiCell) {
        result.getList[Long](result.fieldIndex("writeTimeResult")) should contain theSameElementsAs Seq(1000, 1000)
      } else {
        result.get(result.fieldIndex("writeTimeResult")).asInstanceOf[AnyRef] should be (1000)
      }
    }
  }

  it should "be able to read multiple TTLs" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks,
          "ttl.v" -> "ttlOfV",
          "ttl.v2" -> "ttlOfV2"
        )
      )
      .load()

    val result = df.select(sum("ttlOfV").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L

    val result2 = df.select(sum("ttlOfV2").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
  }

  it should "be able to read TTL using the function api" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks
        )
      )
      .load()
      .select(ttl("v"))

    val result = df.select(sum("TTL(V)").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "be able to read TTL using the function api and get the column" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks
        )
      )
      .load()
      .select(col("v"), ttl("v"))

    val result = df.select(sum("TTL(V)").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "be able to read TTL using the function api and get the column from subsequent call" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks
        )
      )
      .load()
      .select(col("v"))
      .select(ttl("v"))

    val result = df.select(sum("TTL(V)").as("sum"))
      .collect()
      .head
      .getAs[Long]("sum")
    result should be > 0L
  }

  it should "return null TTL for lit(null) column" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks
        )
      )
      .load().select("v").limit(1)

    val spark = sparkSession
    import spark.implicits._
    val nullDf = (Seq((1))).toDF("n").select (lit(null) as "v")


    val unionDf = (df union nullDf).select(ttl("v") as ("ttl"))
    val result = unionDf.collect()

    result(0).getAs[Int](0) should be > 0
    result(1).getAs[AnyRef](0) should be (null)
  }

    it should "return null TTL for lit(null) reversed column" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks
        )
      )
      .load().select("v").limit(1)

    val spark = sparkSession
    import spark.implicits._
    val nullDf = (Seq((1))).toDF("n").select (lit(null) as "v")

    val unionDf = (nullDf union df).select(ttl("v") as ("ttl"))
    val result = unionDf.collect()

    result(1).getAs[Int](0) should be > 0
    result(0).getAs[AnyRef](0) should be (null)
  }

  it should "fail trying to read TTL from non-regular columns" in intercept[IllegalArgumentException]{
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks,
          "ttl.v" -> "ttlOfV",
          "ttl.c" -> "ttlOfC"
        )
      ).load()
  }

  it should "fail trying to read TTL from non-regular columns with the function api" in intercept[IllegalArgumentException]{
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks
        )
      )
      .load()
      .select(ttl("k"))
  }

  it should "throw an exception when reading writetime from non-regular columns" in intercept[IllegalArgumentException]{
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "basic",
          "keyspace" -> ks,
          "writetime.v" -> "writeTimeOfV",
          "writetime.c" -> "writeTimeOfC"
        )
      )
      .load()
  }

  it should "be able to write ttl" in {
    sparkSession
      .createDataFrame(Seq((-1,-1,-1,10000)))
      .toDF("k","c","v","ttlCol")
      .write
      .cassandraFormat("basic", ks)
      .option("ttl", "ttlCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -1"))
    result.one().getInt("t") shouldBe (9000 +- 1000)
  }

  it should "be able to write ttl as first column" in {
    sparkSession
      .createDataFrame(Seq((10000,-1,-1,-2)))
      .toDF("ttlCol","k","c","v")
      .write
      .cassandraFormat("basic", ks)
      .option("ttl", "ttlCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -1"))
    result.one().getInt("t") shouldBe (9000 +- 1000)
  }

  it should "be able to write ttl literals" in {
     sparkSession
      .createDataFrame(Seq((-500,-1,-1)))
      .toDF("k","c","v")
      .write
      .cassandraFormat("basic", ks)
      .withTTL(50000)
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -500"))
    result.one().getInt("t") shouldBe (50000 +- 1000)
  }

  it should "be able to write ttl withTTL" in {
    sparkSession
      .createDataFrame(Seq((-50,-1,-1,10000)))
      .toDF("k","c","v","ttlCol")
      .write
      .cassandraFormat("basic", ks)
      .withTTL("ttlCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT TTL(v) as t FROM $ks.basic WHERE k = -50"))
    result.one().getInt("t") shouldBe (9000 +- 1000)
  }

   it should "be able to write withWritetime" in {
    sparkSession
      .createDataFrame(Seq((-501,-2,-2)))
      .toDF("k","c","v")
      .write
      .cassandraFormat("basic", ks)
      .withWriteTime(5000)
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT WRITETIME(v) as t FROM $ks.basic WHERE k = -501"))
    result.one().getLong("t") shouldBe (5000L)
  }

  it should "be able to write writeTime literals" in {
    sparkSession
      .createDataFrame(Seq((-51,-2,-2,10000)))
      .toDF("k","c","v","writetimeCol")
      .write
      .cassandraFormat("basic", ks)
      .withWriteTime("writetimeCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT WRITETIME(v) as t FROM $ks.basic WHERE k = -51"))
    result.one().getLong("t") shouldBe (10000L)
  }

  it should "throw an exception when attempting to use withWriteTime or withTTL on non-Cassandra sources" in {
     intercept[IllegalArgumentException] {
       sparkSession
         .createDataFrame(Seq((-51,-2,-2,10000)))
         .toDF("k","c","v","writetimeCol")
         .write
         .withWriteTime("writetimeCol")
         .mode("APPEND")
         .save()
     }
    intercept[IllegalArgumentException] {
       sparkSession
         .createDataFrame(Seq((-51,-2,-2,10000)))
         .toDF("k","c","v","writetimeCol")
         .write
         .withWriteTime("writetimeCol")
         .mode("APPEND")
         .save()
     }
  }


  it should "be able to write writetime" in {
    sparkSession
      .createDataFrame(Seq((-2,-2,-2,10000)))
      .toDF("k","c","v","writetimeCol")
      .write
      .cassandraFormat("basic", ks)
      .option("writetime", "writetimeCol")
      .mode("APPEND")
      .save()

    val result = conn.withSessionDo(_.execute(s"SELECT WRITETIME(v) as t FROM $ks.basic WHERE k = -2"))
    result.one().getLong("t") shouldBe (10000L)
  }

  "Spark SQL" should "be able to read TTL" in {
    sparkSession.sql(s"SELECT sum(ttl(v)) FROM $ks.basic")
      .collect()
      .head.getLong(0) should be > 1000L
  }

  it should "be able to read WRITETIME" in {
    sparkSession.sql(s"SELECT sum(writetime(v)) FROM $ks.basic")
      .collect()
      .head.getLong(0) should be > 1000L
  }

  it should "be able to read TTL from case sensitive column" in {
    sparkSession.sql(s"SELECT ttl(Value) FROM $ks.caseNames")
      .collect()
      .head.getInt(0) should be > 1000
  }

  it should "be able to read WRITETIME from case sensitive column" in {
    sparkSession.sql(s"SELECT writetime(`Dot.Value`) FROM $ks.caseNames")
      .collect()
      .head.getLong(0) should be (10000L)
  }

  it should "be able to read case sensitive column TTL from options" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "caseNames",
          "keyspace" -> ks,
          "ttl.Value" -> "ttlOfV"
        )
      )
      .load()

    val result = df.select("ttlOfV")
      .collect()
      .head
      .getAs[Int](0)
    result should be > 0
  }

  it should "be able to read case sensitive column WRITETIME from options" in {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "caseNames",
          "keyspace" -> ks,
          "writetime.Dot.Value" -> "wtOfV"
        )
      )
      .load()

    val result = df.select("wtOfV")
      .collect()
      .head
      .getAs[Long](0)
    result should be (10000L)
  }

  it should "throw an exception when calling writetime on more than one column" in intercept[AnalysisException] {
    sparkSession.sql(s"SELECT sum(writetime(v, k)) FROM $ks.basic")
  }

  it should "throw an exception when calling ttl on more than one column" in intercept[AnalysisException] {
    sparkSession.sql(s"SELECT sum(ttl(v, k)) FROM $ks.basic")
  }

}

