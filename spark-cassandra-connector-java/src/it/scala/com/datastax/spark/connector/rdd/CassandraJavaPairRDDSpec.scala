package com.datastax.spark.connector.rdd

import java.io.IOException
import java.util

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.{Function => JFunction, Function2}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.japi.CassandraRow
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.util.JavaApiHelper

import org.apache.commons.lang3.tuple
import org.scalatest._

import scala.collection.JavaConversions._
import CassandraJavaUtil._

case class SimpleClass(value: Integer)

class CassandraJavaPairRDDSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll
with ShouldMatchers with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(Set(EmbeddedCassandra.cassandraHost))

  conn.withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS java_api_test")
    session.execute("CREATE KEYSPACE java_api_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE java_api_test.test_table_2 (key TEXT, key2 TEXT, value INT, PRIMARY KEY (key, key2))")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('a', 'x', 1)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('a', 'y', 2)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('a', 'z', 3)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('b', 'x', 4)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('b', 'y', 5)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('b', 'z', 6)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('c', 'x', 7)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('c', 'y', 8)")
    session.execute("INSERT INTO java_api_test.test_table_2 (key, key2, value) VALUES ('c', 'z', 9)")

  }

  "CassandraJavaPairRDD" should "allow to reduce by key " in {
    val rows = javaFunctions(sc).cassandraTable("java_api_test", "test_table_2",
      mapColumnTo(classOf[java.lang.String]), mapRowTo(classOf[SimpleClass])).select("key2", "value")

    val reduced = rows.reduceByKey(new Function2[SimpleClass, SimpleClass, SimpleClass] {
      override def call(v1: SimpleClass, v2: SimpleClass): SimpleClass = SimpleClass(v1.value + v2.value)
    })

    val result: util.List[(String, SimpleClass)] = reduced.collect()

    result should have size 3
    result should contain (("x", SimpleClass(12)))
    result should contain (("y", SimpleClass(15)))
    result should contain (("z", SimpleClass(18)))
  }

}
