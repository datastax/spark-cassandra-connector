package com.datastax.spark.connector.writer

import java.io.IOException
import java.net.InetAddress

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.{CassandraServer, SparkServer}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConversions._

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: String)

class TableWriterSpec extends FlatSpec with Matchers with BeforeAndAfter with CassandraServer with SparkServer {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(InetAddress.getByName("127.0.0.1"))

  before {
    conn.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS write_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS write_test.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
      session.execute("CREATE TABLE IF NOT EXISTS write_test.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)")
      session.execute("CREATE TABLE IF NOT EXISTS write_test.blobs (key INT PRIMARY KEY, b blob)")
      session.execute("TRUNCATE write_test.key_value")
      session.execute("TRUNCATE write_test.collections")
      session.execute("TRUNCATE write_test.blobs")
    }
  }

  private def verifyKeyValueTable() {
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.key_value").all()
      result should have size 3
      for (row <- result) {
        Some(row.getInt(0)) should contain oneOf(1, 2, 3)
        Some(row.getLong(1)) should contain oneOf(1, 2, 3)
        Some(row.getString(2)) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  "A TableWriter" should "write RDD of tuples" in {
    val col = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra("write_test", "key_value", Seq("key", "group", "value"))
    verifyKeyValueTable()
  }

  it should "write RDD of tuples applying proper data type conversions" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    sc.parallelize(col).saveToCassandra("write_test", "key_value")
    verifyKeyValueTable()
  }

  it should "write RDD of case class objects" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra("write_test", "key_value")
    verifyKeyValueTable()
  }

  it should "write RDD of case class objects applying proper data type conversions" in {
    val col = Seq(
      KeyValueWithConversion("1", 1, "value1"),
      KeyValueWithConversion("2", 2, "value2"),
      KeyValueWithConversion("3", 3, "value3")
    )
    sc.parallelize(col).saveToCassandra("write_test", "key_value")
    verifyKeyValueTable()
  }

  it should "write RDD of CassandraRow objects" in {
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> 1, "group" -> 1L, "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> 2, "group" -> 2L, "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> 3, "group" -> 3L, "value" -> "value3"))
    )
    sc.parallelize(col).saveToCassandra("write_test", "key_value")
    verifyKeyValueTable()
  }

  it should "write RDD of CassandraRow objects applying proper data type conversions" in {
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> "1", "group" -> BigInt(1), "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> "2", "group" -> BigInt(2), "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> "3", "group" -> BigInt(3), "value" -> "value3"))
    )
    sc.parallelize(col).saveToCassandra("write_test", "key_value")
    verifyKeyValueTable()
  }

  it should "write collections" in {
    val col = Seq(
      (1, Vector("item1", "item2"), Set("item1", "item2"), Map("key1" -> "value1", "key2" -> "value2")),
      (2, Vector.empty[String], Set.empty[String], Map.empty[String, String]))
    sc.parallelize(col).saveToCassandra("write_test", "collections", Seq("key", "l", "s", "m"))

    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.collections").all()
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getList("l", classOf[String]).toSeq shouldEqual Seq("item1", "item2")
      row0.getSet("s", classOf[String]).toSeq shouldEqual Seq("item1", "item2")
      row0.getMap("m", classOf[String], classOf[String]).toMap shouldEqual Map("key1" -> "value1", "key2" -> "value2")
      row1.isNull("l") shouldEqual true
      row1.isNull("m") shouldEqual true
      row1.isNull("s") shouldEqual true
    }
  }

  it should "write blobs" in {
    val col = Seq((1, Some(Array[Byte](0, 1, 2, 3))), (2, None))
    sc.parallelize(col).saveToCassandra("write_test", "blobs", Seq("key", "b"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.blobs").all()
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getBytes("b").remaining shouldEqual 4
      row1.isNull("b") shouldEqual true
    }
  }

  it should "throw IOException if table is not found" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    intercept[IOException] {
      sc.parallelize(col).saveToCassandra("write_test", "unknown_table")
    }
  }

}
