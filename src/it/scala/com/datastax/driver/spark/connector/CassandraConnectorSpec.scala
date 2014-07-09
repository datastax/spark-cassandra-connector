package com.datastax.driver.spark.connector

import java.net.InetAddress

import com.datastax.driver.spark.util.CassandraServer
import org.scalatest.{FlatSpec, Matchers}

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)

class CassandraConnectorSpec extends FlatSpec with Matchers with CassandraServer {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(InetAddress.getByName("127.0.0.1"))

  val createKeyspaceCql = "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"

  "A CassandraConnector" should "connect to Cassandra with native protocol" in {
    conn.withSessionDo { session =>
      assert(session.isClosed === false)
      assert(session !== null)
    }
  }

  it should "connect to Cassandra with thrift" in {
    conn.withCassandraClientDo { client =>
      assert(client.describe_cluster_name() === "Test Cluster")
    }
  }

  it should "give access to cluster metadata" in {
    conn.withClusterDo { cluster =>
      assert(cluster.getMetadata.getClusterName === "Test Cluster")
      assert(cluster.getMetadata.getAllHosts.size > 0)
    }
  }

  it should "run queries" in {
    conn.withSessionDo { session =>
      session.execute(createKeyspaceCql)
      session.execute("DROP TABLE IF EXISTS test.simple_query")
      session.execute("CREATE TABLE test.simple_query (key INT PRIMARY KEY, value TEXT)")
      session.execute("INSERT INTO test.simple_query(key, value) VALUES (1, 'value')")
      val result = session.execute("SELECT * FROM test.simple_query WHERE key = ?", 1.asInstanceOf[AnyRef])
      assert(result.one().getString("value") === "value")
    }
  }

  it should "cache PreparedStatements" in {
    conn.withSessionDo { session =>
      session.execute(createKeyspaceCql)
      session.execute("DROP TABLE IF EXISTS test.pstmt")
      session.execute("CREATE TABLE test.pstmt (key INT PRIMARY KEY, value TEXT)")
      val stmt1 = session.prepare("INSERT INTO test.pstmt (key, value) VALUES (?, ?)")
      val stmt2 = session.prepare("INSERT INTO test.pstmt (key, value) VALUES (?, ?)")
      assert(stmt1 eq stmt2)
    }
  }

  it should "disconnect from the cluster after use" in {
    val cluster = conn.withClusterDo { cluster => cluster }
    Thread.sleep(CassandraConnector.keepAliveMillis * 2)
    assert(cluster.isClosed === true)
  }


}


