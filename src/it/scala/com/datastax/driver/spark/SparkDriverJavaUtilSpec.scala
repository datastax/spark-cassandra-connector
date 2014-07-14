package com.datastax.driver.spark

import java.net.InetAddress

import com.datastax.driver.spark.connector.CassandraConnector
import com.datastax.driver.spark.util.{CassandraServer, SparkServer}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConversions._

class SparkDriverJavaUtilSpec extends FlatSpec with Matchers with BeforeAndAfter with CassandraServer with SparkServer {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(InetAddress.getByName("127.0.0.1"))

  before {
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS java_api_test")
      session.execute("CREATE KEYSPACE java_api_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE java_api_test.test_table (key INT, value TEXT, PRIMARY KEY (key))")
    }
  }

  "A SparkDriverJavaUtil" should "allow to save beans to Cassandra" in {
    val beansRdd = sc.parallelize(Seq(
      SampleJavaBean.newInstance(1, "one"),
      SampleJavaBean.newInstance(2, "two"),
      SampleJavaBean.newInstance(3, "three")
    ))

    CassandraJavaUtil.javaFunctions(beansRdd, classOf[SampleJavaBean])
      .saveToCassandra("java_api_test", "test_table", CassandraJavaUtil.NO_OVERRIDE)

    val results = conn.withSessionDo(_.execute("SELECT * FROM java_api_test.test_table"))

    val rows = results.all()
    assert(rows.size() == 3)
    assert(rows.exists(row => row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row => row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row => row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to read data as CassandraRows " in {
    conn.withSessionDo { session =>
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (1, 'one')")
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (2, 'two')")
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (3, 'three')")
    }

    val rows = CassandraJavaUtil.javaFunctions(sc).cassandraTable("java_api_test", "test_table").toArray()
    assert(rows.size == 3)
    assert(rows.exists(row => row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row => row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row => row.getString("value") == "three" && row.getInt("key") == 3))
  }

  it should "allow to read data as Java beans " in {
    conn.withSessionDo { session =>
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (1, 'one')")
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (2, 'two')")
      session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (3, 'three')")
    }

    val beans = CassandraJavaUtil.javaFunctions(sc).cassandraTable("java_api_test", "test_table", classOf[SampleJavaBean]).toArray()
    assert(beans.size == 3)
    assert(beans.exists(bean => bean.getValue == "one" && bean.getKey == 1))
    assert(beans.exists(bean => bean.getValue == "two" && bean.getKey == 2))
    assert(beans.exists(bean => bean.getValue == "three" && bean.getKey == 3))
  }
}
