package com.datastax.spark.connector.rdd

import java.util

import scala.collection.JavaConversions._
import scala.concurrent.Future

import org.apache.spark.api.java.function.{Function => JFunction, Function2}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.japi.CassandraJavaUtil._

case class SimpleClass(value: Integer)

class CassandraJavaPairRDDSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    try {
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(s"CREATE TABLE $ks.test_table_1 (key TEXT, key2 TEXT, value INT, PRIMARY KEY (key, key2))")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('a', 'x', 1)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('a', 'y', 2)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('a', 'z', 3)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('b', 'x', 4)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('b', 'y', 5)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('b', 'z', 6)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('c', 'x', 7)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('c', 'y', 8)")
          session.execute(s"INSERT INTO $ks.test_table_1 (key, key2, value) VALUES ('c', 'z', 9)")
        },
         Future {
          session.execute(s"CREATE TABLE $ks.test_table_2 (key TEXT, key2 TEXT, value INT, PRIMARY KEY (key, key2))")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('a', 'x', 1)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('a', 'y', 2)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('a', 'z', 3)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('b', 'x', 4)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('b', 'y', 5)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('b', 'z', 6)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('c', 'x', 7)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('c', 'y', 8)")
          session.execute(s"INSERT INTO $ks.test_table_2 (key, key2, value) VALUES ('c', 'z', 9)")
        },

        Future {
          session.execute(s"CREATE TABLE IF NOT EXISTS $ks.wide_rows(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (10, 10, '1010')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (10, 11, '1011')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (10, 12, '1012')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (20, 20, '2020')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (20, 21, '2021')")
          session.execute(s"INSERT INTO $ks.wide_rows(key, group, value) VALUES (20, 22, '2022')")
        }
      )
    } catch {
      case ex: Exception =>
        ex.printStackTrace(System.err)
        throw ex
    }
  }

  "CassandraJavaPairRDD" should "allow to reduce by key " in {
    val rows = javaFunctions(sc)
      .cassandraTable(ks, "test_table_1", mapRowTo(classOf[SimpleClass]))
      .select("key2", "value")
      .keyBy(
        mapColumnTo(classOf[String]),
        mapToRow(classOf[String]),
        classOf[String],
        "key2")

    val reduced = rows.reduceByKey(new Function2[SimpleClass, SimpleClass, SimpleClass] {
      override def call(v1: SimpleClass, v2: SimpleClass): SimpleClass = SimpleClass(v1.value + v2.value)
    })

    val result: util.List[(String, SimpleClass)] = reduced.collect()

    result should have size 3
    result should contain(("x", SimpleClass(12)))
    result should contain(("y", SimpleClass(15)))
    result should contain(("z", SimpleClass(18)))
  }

  it should "allow to use spanBy method" in {
    val f = new JFunction[(Integer, Integer), Integer]() {
      override def call(row: (Integer, Integer)) = row._1
    }

    val results = javaFunctions(sc)
      .cassandraTable(ks, "wide_rows", mapColumnTo(classOf[Integer]))
      .select("group", "key")
      .keyBy(
        mapColumnTo(classOf[Integer]),
        mapToRow(classOf[Integer]),
        classOf[Integer],
        "key")
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
    val f = new JFunction[(Integer, Integer), Integer]() {
      override def call(row: (Integer, Integer)) = row._1
    }

    val results = javaFunctions(sc)
      .cassandraTable(ks, "wide_rows", mapColumnTo(classOf[Integer]))
      .select("group", "key")
      .keyBy(
        mapColumnTo(classOf[Integer]),
        mapToRow(classOf[Integer]),
        classOf[Integer],
        "key")
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

  it should "allow to use of keyByAndApplyPartitioner" in {

    val rdd1 = javaFunctions(sc)
      .cassandraTable(ks, "test_table_1", mapColumnTo(classOf[(Integer)]))
      .select("value", "key")
      .keyBy(
        mapColumnTo(classOf[String]),
        mapToRow(classOf[String]),
        classOf[String],
        "key")

    val rdd2 = javaFunctions(sc)
      .cassandraTable(ks, "test_table_2", mapColumnTo(classOf[(Integer)]))
      .select("value", "key")
      .keyAndApplyPartitionerFrom(
        mapColumnTo(classOf[String]),
        mapToRow(classOf[String]),
        classOf[String],
        rdd1)

    val joinRDD = rdd1.join(rdd2)
    joinRDD.toDebugString should not contain ("+-")
    val results = joinRDD.values.collect()
    results should have length (27)
  }


}
