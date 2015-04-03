package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import org.apache.spark.SparkConf
import com.datastax.spark.connector.embedded._

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)

class CassandraConnectorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  val createKeyspaceCql = "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"

  "A CassandraConnector" should "connect to Cassandra with native protocol" in {
    conn.withSessionDo { session =>
      assert(session.isClosed === false)
      assert(session !== null)
    }
  }

  it should "give access to cluster metadata" in {
    conn.withClusterDo { cluster =>
      assert(cluster.getMetadata.getClusterName === "Test Cluster0")
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

  it should "share internal Cluster and Session object between multiple logical sessions" in {
    val session1 = conn.openSession()
    val threadCount1 = Thread.activeCount()
    val session2 = conn.openSession()
    val threadCount2 = Thread.activeCount()
    session1.getCluster should be theSameInstanceAs session2.getCluster
    // Unfortunately we don't have a way to obtain an internal Session object; all we got here are proxies.
    // Instead, we try to figure out whether a new Session was opened by counting active threads.
    // Opening internal Session creates new threads, so if none was created, the thread count would not change.
    threadCount1 shouldEqual threadCount2
    session1.close()
    session1.isClosed shouldEqual true
    session2.isClosed shouldEqual false
    session2.close()
    session2.isClosed shouldEqual true
  }

  it should "share internal Cluster object between multiple logical sessions created by different connectors to the same cluster" in {
    val conn2 = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
    val session1 = conn.openSession()
    val threadCount1 = Thread.activeCount()
    val session2 = conn2.openSession()
    val threadCount2 = Thread.activeCount()
    session1.getCluster should be theSameInstanceAs session2.getCluster
    threadCount1 shouldEqual threadCount2
    session1.close()
    session1.isClosed shouldEqual true
    session2.isClosed shouldEqual false
    session2.close()
    session2.isClosed shouldEqual true
  }

  it should "be configurable from SparkConf" in {
    val host = EmbeddedCassandra.getHost(0).getHostAddress
    val conf = new SparkConf(loadDefaults = true)
      .set(CassandraConnectorConf.CassandraConnectionHostProperty, host)

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }

  it should "accept multiple hostnames in spark.cassandra.connection.host property" in {
    val goodHost = EmbeddedCassandra.getHost(0).getHostAddress
    val invalidHost = "192.168.254.254"
    // let's connect to two addresses, of which the first one is deliberately invalid
    val conf = new SparkConf(loadDefaults = true)
      .set(CassandraConnectorConf.CassandraConnectionHostProperty, invalidHost + "," + goodHost)

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }
}


