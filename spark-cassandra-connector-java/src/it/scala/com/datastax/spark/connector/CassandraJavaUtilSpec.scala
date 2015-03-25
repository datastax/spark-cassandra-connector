package com.datastax.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.japi.CassandraJavaUtil._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConversions._

class CassandraJavaUtilSpec extends SparkCassandraITFlatSpecBase with BeforeAndAfter {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  before {
    conn.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS java_api_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute("DROP TABLE IF EXISTS java_api_test.test_table")
      session.execute("CREATE TABLE java_api_test.test_table (key INT, value TEXT, PRIMARY KEY (key))")
      session.execute("DROP TABLE IF EXISTS java_api_test.test_table2")
      session.execute("CREATE TABLE java_api_test.test_table2 (key INT, value TEXT, sub_class_field TEXT, PRIMARY KEY (key))")
    }
  }

  "CassandraJavaUtil" should "allow to save beans (with multiple constructors) to Cassandra" in {
    assert(conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table")).all().isEmpty)

    val beansRdd: RDD[SampleJavaBeanWithMultipleCtors] = sc.parallelize(Seq(
      new SampleJavaBeanWithMultipleCtors(1, "one"),
      new SampleJavaBeanWithMultipleCtors(2, "two"),
      new SampleJavaBeanWithMultipleCtors(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder("java_api_test", "test_table", mapToRow(classOf[SampleJavaBeanWithMultipleCtors]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }


  it should "allow to save beans to Cassandra" in {
    assert(conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table")).all().isEmpty)

    val beansRdd = sc.parallelize(Seq(
      SampleJavaBean.newInstance(1, "one"),
      SampleJavaBean.newInstance(2, "two"),
      SampleJavaBean.newInstance(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder("java_api_test", "test_table", mapToRow(classOf[SampleJavaBean]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to save beans with transient fields to Cassandra" in {
    assert(conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table")).all().isEmpty)

    val beansRdd = sc.parallelize(Seq(
      SampleJavaBeanWithTransientFields.newInstance(1, "one"),
      SampleJavaBeanWithTransientFields.newInstance(2, "two"),
      SampleJavaBeanWithTransientFields.newInstance(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder("java_api_test", "test_table", mapToRow(classOf[SampleJavaBeanWithTransientFields]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to save beans with inherited fields to Cassandra" in {
    assert(conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table2")).all().isEmpty)

    val beansRdd = sc.parallelize(Seq(
      SampleJavaBeanSubClass.newInstance(1, "one", "a"),
      SampleJavaBeanSubClass.newInstance(2, "two", "b"),
      SampleJavaBeanSubClass.newInstance(3, "three", "c")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder("java_api_test", "test_table2", mapToRow(classOf[SampleJavaBeanSubClass]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table2"))
    val rows = results.all()

    rows should have size 3
    rows.map(row => (row.getString("value"), row.getInt("key"), row.getString("sub_class_field"))).toSet shouldBe Set(
      ("one", 1, "a"),
      ("two", 2, "b"),
      ("three", 3, "c")
    )
  }

  it should "allow to save nested beans to Cassandra" in {
    assert(conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table")).all().isEmpty)

    val outer = new SampleWithNestedJavaBean

    val beansRdd = sc.parallelize(Seq(
      new outer.InnerClass(1, "one"),
      new outer.InnerClass(2, "two"),
      new outer.InnerClass(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd)
      .writerBuilder("java_api_test", "test_table", mapToRow(classOf[outer.InnerClass]))
      .saveToCassandra()

    val results = conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row ⇒ row.getString("value") == "three" && row.getInt("key") == 3))
  }

}
