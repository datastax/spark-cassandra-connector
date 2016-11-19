package com.datastax.spark.connector.cql

import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}
import com.datastax.spark.connector.embedded._

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)

class CassandraConnectorSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

  val createKeyspaceCql = keyspaceCql(ks)

  "A CassandraConnector" should "connect to Cassandra with native protocol" in {
    conn.withSessionDo { session =>
      assert(session.isClosed === false)
      assert(session !== null)
    }
  }

  it should "give access to cluster metadata" in {
    conn.withClusterDo { cluster =>
      assert(cluster.getMetadata.getClusterName != null)
      assert(cluster.getMetadata.getAllHosts.size > 0)
    }
  }

  it should "run queries" in {
    conn.withSessionDo { session =>
      session.execute(createKeyspaceCql)
      session.execute(s"DROP TABLE IF EXISTS $ks.simple_query")
      session.execute(s"CREATE TABLE $ks.simple_query (key INT PRIMARY KEY, value TEXT)")
      session.execute(s"INSERT INTO $ks.simple_query(key, value) VALUES (1, 'value')")
      val result = session.execute(s"SELECT * FROM $ks.simple_query WHERE key = ?", 1.asInstanceOf[AnyRef])
      assert(result.one().getString("value") === "value")
    }
  }

  it should "cache PreparedStatements" in {
    conn.withSessionDo { session =>
      session.execute(createKeyspaceCql)
      session.execute(s"DROP TABLE IF EXISTS $ks.pstmt")
      session.execute(s"CREATE TABLE $ks.pstmt (key INT PRIMARY KEY, value TEXT)")
      val stmt1 = session.prepare(s"INSERT INTO $ks.pstmt (key, value) VALUES (?, ?)")
      val stmt2 = session.prepare(s"INSERT INTO $ks.pstmt (key, value) VALUES (?, ?)")
      assert(stmt1 eq stmt2)
    }
  }

  it should "disconnect from the cluster after use" in {
    val cluster = conn.withClusterDo { cluster => cluster }
    Thread.sleep(
      sc.getConf.getInt(
        "spark.cassandra.connection.keep_alive_ms",
        CassandraConnectorConf.KeepAliveMillisParam.default) * 2)
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
    val conn2 = CassandraConnector(sc.getConf)
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

  it should "cache session objects for reuse" in {
    CassandraConnector(sc.getConf).withSessionDo(x => {})
    CassandraConnector(sc.getConf).withSessionDo(x => {})
    val sessionCache = CassandraConnector.sessionCache
    sessionCache.contains(CassandraConnectorConf(sc.getConf)) should be (true)
    sessionCache.cache.size should be (1)
  }

  it should "not make multiple clusters when writing multiple RDDs" in {
    CassandraConnector(sc.getConf).withSessionDo{ session =>
      session.execute(createKeyspaceCql)
      session.execute(s"CREATE TABLE IF NOT EXISTS $ks.pair (x int, y int, PRIMARY KEY (x))")
    }
    for (trial <- 1 to 3){
      val rdd = sc.parallelize(1 to 100).map(x=> (x,x)).saveToCassandra(ks, "pair")
    }
    val sessionCache = CassandraConnector.sessionCache
    sessionCache.contains(CassandraConnectorConf(sc.getConf)) should be (true)
    sessionCache.cache.size should be (1)
  }

  it should "be configurable from SparkConf" in {
    val conf = sc.getConf

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }

  it should "accept multiple hostnames in spark.cassandra.connection.host property" in {
    val goodHost = EmbeddedCassandra.getHost(0).getHostAddress
    val invalidHost = "192.168.254.254"
    // let's connect to two addresses, of which the first one is deliberately invalid
    val conf = sc.getConf
    conf.set(CassandraConnectorConf.ConnectionHostParam.name, invalidHost + "," + goodHost)

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }

  it should "use compression when configured" in {
    val conf = sc.getConf
      .set(CassandraConnectorConf.CompressionParam.name, "SNAPPY")

    val conn = CassandraConnector(conf)
    conn.withSessionDo { session ⇒
      session.getCluster.getConfiguration
          .getProtocolOptions.getCompression shouldBe ProtocolOptions.Compression.SNAPPY
    }
  }
}


