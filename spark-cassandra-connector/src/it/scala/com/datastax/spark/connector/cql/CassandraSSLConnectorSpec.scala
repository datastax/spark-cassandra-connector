package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.embedded.EmbeddedCassandra

class CassandraSSLConnectorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-ssl.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)),
    sslEnabled = true, sslTrustStorePath = Some("truststore"), sslTrustStorePassword = Some("connector"))

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
