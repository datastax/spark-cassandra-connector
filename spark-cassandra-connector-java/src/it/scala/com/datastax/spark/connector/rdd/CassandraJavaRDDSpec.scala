package com.datastax.spark.connector.rdd

import scala.collection.JavaConversions._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector.embedded._

class CassandraJavaRDDSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(EmbeddedCassandra.cassandraHost)

  before {
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS java_api_test")
      session.execute("CREATE KEYSPACE java_api_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE java_api_test.test_table (key INT, value TEXT, PRIMARY KEY (key))")
      session.execute("CREATE INDEX test_table_idx ON java_api_test.test_table (value)")

      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (1, 'one')")
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (2, 'two')")
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (3, 'three')")
    }
  }

  "CassandraJavaRDD" should "allow to select a subset of columns" in {
    val rows = CassandraJavaUtil.javaFunctions(sc).cassandraTable("java_api_test", "test_table")
      .select("key").toArray()
    assert(rows.size == 3)
    assert(rows.exists(row => !row.contains("value") && row.getInt("key") == 1))
    assert(rows.exists(row => !row.contains("value") && row.getInt("key") == 2))
    assert(rows.exists(row => !row.contains("value") && row.getInt("key") == 3))
  }

  "CassandraJavaRDD" should "return selected columns" in {
    val rdd = CassandraJavaUtil.javaFunctions(sc).cassandraTable("java_api_test", "test_table")
      .select("key")
    assert(rdd.selectedColumnNames().size == 1)
    assert(rdd.selectedColumnNames().contains("key"))
  }

  "CassandraJavaRDD" should "allow to use where clause to filter records" in {
    val rows = CassandraJavaUtil.javaFunctions(sc).cassandraTable("java_api_test", "test_table")
      .where("value = ?", "two").toArray()
    assert(rows.size == 1)
    assert(rows.exists(row => row.getString("value") == "two" && row.getInt("key") == 2))
  }

}
