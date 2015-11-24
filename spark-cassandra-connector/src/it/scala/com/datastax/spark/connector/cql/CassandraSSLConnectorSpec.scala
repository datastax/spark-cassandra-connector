package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.embedded.EmbeddedCassandra

class CassandraSSLConnectorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-ssl.yaml.template"))

  // TODO: Add a test so that CassandraConnector is configured for SSL through SparkConf

  val conn = CassandraConnector(
    hosts = Set(EmbeddedCassandra.getHost(0)),
    port = EmbeddedCassandra.getPort(0),
    cassandraSSLConf = CassandraSSLConf(
      enabled = true,
      trustStorePath = Some(ClassLoader.getSystemResource("truststore").getPath),
      trustStorePassword = Some("connector"),
      enabledAlgorithms = Set("TLS_RSA_WITH_AES_128_CBC_SHA")),
    connectTimeoutMillis = 30000)

  "A CassandraConnector" should "be able to use a secure connection when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName === "Test Cluster 0")
    }
  }

}
