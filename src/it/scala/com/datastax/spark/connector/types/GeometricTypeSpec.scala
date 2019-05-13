/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.types

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import com.datastax.driver.core.Session
import com.datastax.driver.dse.geometry._
import com.datastax.driver.dse.geometry.codecs.{LineStringCodec, PointCodec, PolygonCodec}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations

class GeometricTypeSpec extends DseITFlatSpecBase {

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(sparkConf)

  override lazy val conn = CassandraConnector(sparkConf)

  def makeGeometricTables(session: Session): Unit = {
    session.execute(
      s"""CREATE TABLE $ks.geom
          |(k INT PRIMARY KEY,
          | pnt 'PointType',
          | line 'LineStringType',
          | poly 'PolygonType') """.stripMargin)

    session.execute(
      s"""
         |Insert INTO $ks.geom (k, pnt, line, poly) VALUES
         |(1,
         |'POINT (1.1 2.2)',
         |'LINESTRING (30 10, 10 30, 40 40)',
         |'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')
       """.stripMargin
    )
  }

  var spark: SparkSession = _

  beforeClass {

    println(sparkConf.toDebugString)
    spark = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "file:///tmp/")
      .getOrCreate()

    conn.withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
            |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
            .stripMargin)
      makeGeometricTables(session)
    }
  }

  "The Spark Cassandra Connector" should "find a converter for Point types" in {
    ColumnType.fromDriverType(PointCodec.DATA_TYPE) should be(PointType)
  }

  it should "find a converter for Polygon types" in {
    ColumnType.fromDriverType(PolygonCodec.DATA_TYPE) should be(PolygonType)
  }

  it should "find a converter for LineString types" in {
    ColumnType.fromDriverType(LineStringCodec.DATA_TYPE) should be(LineStringType)
  }

  import com.datastax.spark.connector.types.DseTypeConverter.{LineStringConverter, PointConverter, PolygonConverter}

  it should "read point types" in {
    val result = sc.cassandraTable(ks, "geom").select("pnt").collect
    val resultCC = sc.cassandraTable[(Point)](ks, "geom").select("pnt").collect
    val expected = new Point(1.1, 2.2)
    result(0).get[Point](0) should be(expected)
    resultCC(0) should be(expected)
  }

  it should "read polygon types" in {
    val result = sc.cassandraTable(ks, "geom").select("poly").collect
    val resultCC = sc.cassandraTable[(Polygon)](ks, "geom").select("poly").collect
    val expected = new Polygon(
      new Point(30, 10),
      new Point(40, 40),
      new Point(20, 40),
      new Point(10, 20),
      new Point(30, 10))

    result(0).get[Polygon](0) should be(expected)
    resultCC(0) should be(expected)
  }

  it should "read linestring types" in {
    val result = sc.cassandraTable(ks, "geom").select("line").collect
    val resultCC = sc.cassandraTable[(LineString)](ks, "geom").select("line").collect
    val expected = new LineString(
      new Point(30, 10),
      new Point(10, 30),
      new Point(40, 40))

    result(0).get[LineString](0) should be(expected)
    resultCC(0) should be(expected)
  }

  it should "write point types" in {
    val expected = new Point(1.1, 2.2)
    sc.parallelize(Seq((2, expected))).saveToCassandra(ks, "geom", SomeColumns("k", "pnt"))
    val result = conn.withSessionDo(_.execute(s"SELECT pnt FROM $ks.geom where k = 2").one())
    result.get[Point](0, classOf[Point]) should be(expected)
  }

  it should "write polygon types" in {
    val expected = new Polygon(
      new Point(30, 10),
      new Point(40, 40),
      new Point(20, 40),
      new Point(10, 20),
      new Point(30, 10))

    sc.parallelize(Seq((2, expected))).saveToCassandra(ks, "geom", SomeColumns("k", "poly"))
    val result = conn.withSessionDo(_.execute(s"SELECT poly FROM $ks.geom where k = 2").one())
    result.get[Polygon](0, classOf[Polygon]) should be(expected)
  }

  it should "write linestring types" in {
    val expected = new LineString(
      new Point(30, 10),
      new Point(10, 30),
      new Point(40, 40))

    sc.parallelize(Seq((2, expected))).saveToCassandra(ks, "geom", SomeColumns("k", "line"))
    val result = conn.withSessionDo(_.execute(s"SELECT line FROM $ks.geom where k = 2").one())
    result.get[LineString](0, classOf[LineString]) should be(expected)
  }

  def readGeoDF: DataFrame = spark.read.format("org.apache.spark.sql.cassandra").option("table", "geom").option("keyspace", ks).load()

  "SparkSQL" should "read point types" in {
    val result = readGeoDF.select("pnt").collect
    val expected = new Point(1.1, 2.2)
    Point.fromWellKnownText(result(0).getString(0)) should be(expected)
  }

  it should "read polygon types" in {
    val result = readGeoDF.select("poly").collect
    val expected = new Polygon(
      new Point(30, 10),
      new Point(40, 40),
      new Point(20, 40),
      new Point(10, 20),
      new Point(30, 10))
    Polygon.fromWellKnownText(result(0).getString(0)) should be(expected)
  }

  it should "read linestring types" in {
    val result = readGeoDF.select("line").collect
    val expected = new LineString(
      new Point(30, 10),
      new Point(10, 30),
      new Point(40, 40))
    LineString.fromWellKnownText(result(0).getString(0)) should be(expected)
  }

  def writeGeoDF(df: DataFrame): Unit = df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "geom", "keyspace" -> ks)).save

  it should "write point types" in {
    val expected = new Point(1.1, 2.2)
    val df = spark.createDataFrame(Seq((3, expected.toString))).select(col("_1") as "k", col("_2") as "pnt")
    writeGeoDF(df)
    val result = conn.withSessionDo(_.execute(s"SELECT pnt FROM $ks.geom where k = 3").one())
    result.get[Point](0, classOf[Point]) should be(expected)
  }

  it should "write polygon types" in {
    val expected = new Polygon(
      new Point(30, 10),
      new Point(40, 40),
      new Point(20, 40),
      new Point(10, 20),
      new Point(30, 10))
    val df = spark.createDataFrame(Seq((3, expected.toString))).select(col("_1") as "k", col("_2") as "poly")
    writeGeoDF(df)
    val result = conn.withSessionDo(_.execute(s"SELECT poly FROM $ks.geom where k = 3").one())
    result.get[Polygon](0, classOf[Polygon]) should be(expected)
  }

  it should "write linestring types" in {
    val expected = new LineString(
      new Point(30, 10),
      new Point(10, 30),
      new Point(40, 40))
    val df = spark.createDataFrame(Seq((3, expected.toString))).select(col("_1") as "k", col("_2") as "line")
    writeGeoDF(df)
    val result = conn.withSessionDo(_.execute(s"SELECT line FROM $ks.geom where k = 3").one())
    result.get[LineString](0, classOf[LineString]) should be(expected)
  }
}
