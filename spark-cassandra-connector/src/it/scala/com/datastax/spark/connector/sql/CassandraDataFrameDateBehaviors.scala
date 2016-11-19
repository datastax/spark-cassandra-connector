package com.datastax.spark.connector.sql

import java.sql.Date
import java.util.TimeZone

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.joda.time.DateTimeZone
import org.scalatest.FlatSpec
import com.datastax.driver.core.LocalDate
import com.datastax.driver.core.ProtocolVersion._
import com.datastax.spark.connector.SparkCassandraITSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations

trait CassandraDataFrameDateBehaviors extends SparkCassandraITSpecBase {
  this: FlatSpec =>

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

  def dataFrame(timeZone: TimeZone): Unit = skipIfProtocolVersionLT(V4){

    TimeZone.setDefault(timeZone)
    DateTimeZone.setDefault(DateTimeZone.forTimeZone(timeZone))

    val readTable = s"date_test_${timeZone.getID.toLowerCase}_read"
    val writeTable = s"date_test_${timeZone.getID.toLowerCase}_write"

    conn.withSessionDo { session =>
      createKeyspace(session)
      session.execute(s"create table $ks.$readTable (key int primary key, dd date)")
      session.execute(s"insert into $ks.$readTable (key, dd) values (1, '1930-05-31')")
      session.execute(s"create table $ks.$writeTable (key int primary key, d0 date)")
    }

    it should s"read C* LocalDate columns in ${timeZone.getID} timezone" in {
      val df = sparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> readTable, "keyspace" -> ks, "cluster" -> "ClusterOne"))
        .load

      df.count should be(1)

      val foundDate = df.first.getDate(1)
      val foundLocalDate = foundDate.toLocalDate
      val foundTuple = (foundLocalDate.getYear, foundLocalDate.getMonthValue, foundLocalDate.getDayOfMonth)

      val expectedTuple = (1930, 5, 31)

      foundTuple should be(expectedTuple)
    }

    it should s"write java.sql.date to C* date columns in ${timeZone.getID} timezone" in {
      val schema = StructType(Seq(
        StructField("key", DataTypes.IntegerType),
        StructField("d0", DataTypes.DateType)
      ))

      val rows = sc.parallelize(Seq(
        Row(0, Date.valueOf("1986-01-02")),
        Row(1, Date.valueOf("1987-01-02"))
      ))

      val dataFrame = sparkSession.createDataFrame(rows, schema)

      dataFrame.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> writeTable, "keyspace" -> ks, "cluster" -> "ClusterOne"))
        .save

      conn.withSessionDo { session =>
        val count = session.execute(s"select count(1) from $ks.$writeTable").one().getLong(0)
        count should be(2)

        val date = session.execute(s"select d0 from $ks.$writeTable where key = 0").one().getDate(0)
        date should be(LocalDate.fromYearMonthDay(1986, 1, 2))
      }
    }
  }
}
