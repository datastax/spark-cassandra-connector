package com.datastax.spark.connector.rdd

import java.util

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.japi.CassandraJavaUtil._
import org.apache.spark.api.java.function.{Function => JFunction, Function2}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

case class SimpleClass(value: Integer)

class CassandraJavaPairRDDSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

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

  it should "allow to use spanBy method" in {

    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS java_api_test.wide_rows(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (10, 10, '1010')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (10, 11, '1011')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (10, 12, '1012')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (20, 20, '2020')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (20, 21, '2021')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (20, 22, '2022')")
    }

    val f = new JFunction[(Integer, Integer), Integer]() {
      override def call(row: (Integer, Integer)) = row._1
    }

    val results = javaFunctions(sc)
      .cassandraTable("java_api_test", "wide_rows", mapColumnTo(classOf[Integer]), mapColumnTo(classOf[Integer]))
      .select("key", "group")
      .spanBy(f, classOf[Integer])
      .collect()
      .toMap

    results should have size 2
    results should contain key 10
    results should contain key 20
    results(10).size should be(3)
    results(10).map(_._2).toSeq should be(Seq(10, 11, 12))
    results(20).size should be(3)
    results(20).map(_._2).toSeq should be(Seq(20, 21, 22))
  }

  it should "allow to use spanByKey method" in {

    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS java_api_test.wide_rows(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (10, 10, '1010')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (10, 11, '1011')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (10, 12, '1012')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (20, 20, '2020')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (20, 21, '2021')")
      session.execute("INSERT INTO java_api_test.wide_rows(key, group, value) VALUES (20, 22, '2022')")
    }

    val f = new JFunction[(Integer, Integer), Integer]() {
      override def call(row: (Integer, Integer)) = row._1
    }

    val results = javaFunctions(sc)
      .cassandraTable("java_api_test", "wide_rows", mapColumnTo(classOf[Integer]), mapColumnTo(classOf[Integer]))
      .select("key", "group")
      .spanByKey()
      .collect()
      .toMap

    results should have size 2
    results should contain key 10
    results should contain key 20
    results(10).size should be(3)
    results(10).toSeq should be(Seq(10, 11, 12))
    results(20).size should be(3)
    results(20).toSeq should be(Seq(20, 21, 22))
  }


}
