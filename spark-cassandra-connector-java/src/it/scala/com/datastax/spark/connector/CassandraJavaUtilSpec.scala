package com.datastax.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConversions._
import CassandraJavaUtil._

class CassandraJavaUtilSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(Set(cassandraHost))

  before {
    conn.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS java_api_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS java_api_test.test_table (key INT, value TEXT, PRIMARY KEY (key))")
      session.execute("TRUNCATE java_api_test.test_table")
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
