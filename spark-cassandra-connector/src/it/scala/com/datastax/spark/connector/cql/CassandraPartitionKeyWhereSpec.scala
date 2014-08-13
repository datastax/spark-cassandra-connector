package com.datastax.spark.connector.cql

import java.io.IOException
import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.testkit.{CassandraServer, SparkServer}
import com.datastax.spark.connector.types.TypeConverter
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe._

class CassandraPartitionKeyWhereSpec extends FlatSpec with Matchers with CassandraServer with SparkServer {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(cassandraHost)

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS read_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.key2_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    session.execute("INSERT INTO read_test.key2_value (key, group, value) VALUES (1, 100, '0001')")
    session.execute("INSERT INTO read_test.key2_value (key, group, value) VALUES (2, 200, '0002')")
    session.execute("INSERT INTO read_test.key2_value (key, group, value) VALUES (3, 300, '0003')")

   }

  "A cassandraTable" should "allow partition key eq in where" in {
    val rdd = sc.cassandraTable("read_test", "key2_value").where("key = ?", 1)
    val result =  rdd.toArray()
    result should have length 1
    result.head.getInt("key") should be (1)
  }

  "A cassandraTable" should "allow partition key 'in' in where" in {
    val result = sc.cassandraTable("read_test", "key2_value").where("key in (?, ?)", 2,3).toArray()
    result should have length 2
    result.head.getInt("key") should (be (2) or be (3))
  }

  "A cassandraTable" should "allow cluster key 'in' in where" in {
    val result = sc.cassandraTable("read_test", "key2_value").where("group in (?, ?)", 200,300).toArray()
    result should have length 2
    result.head.getInt("key") should (be (2) or be (3))
  }
}
