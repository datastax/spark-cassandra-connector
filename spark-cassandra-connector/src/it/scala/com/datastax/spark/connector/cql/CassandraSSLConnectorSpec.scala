package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.embedded.EmbeddedCassandra

class CassandraSSLConnectorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-ssl.yaml.template"))

  val conn = CassandraConnector(
    hosts = Set(EmbeddedCassandra.getHost(0)),
    cassandraSSLConf = CassandraSSLConf(
      enabled = true,
      trustStorePath = Some(ClassLoader.getSystemResource("truststore").getPath),
      trustStorePassword = Some("connector"),
      enabledAlgorithms = Array("TLS_RSA_WITH_AES_128_CBC_SHA")))

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  "A CassandraConnector" should "be able to use a secure connection when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName === "Test Cluster0")
    }
  }

  it should "be able to use a secure connection when using thrift" in {
    conn.withCassandraClientDo { client =>
      assert(client.describe_cluster_name() === "Test Cluster0")
    }
  }
}
