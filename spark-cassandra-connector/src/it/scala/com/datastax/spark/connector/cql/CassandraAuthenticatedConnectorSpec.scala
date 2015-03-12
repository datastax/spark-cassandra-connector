package com.datastax.spark.connector.cql

import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{Matchers, FlatSpec}

class CassandraAuthenticatedConnectorSpec  extends FlatSpec with Matchers with SharedEmbeddedCassandra {

  useCassandraConfig(Seq("cassandra-password-auth.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)), authConf = PasswordAuthConf("cassandra", "cassandra"))

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName === "Test Cluster0")
    }
  }

  it should "authenticate with username and password when using thrift" in {
    conn.withCassandraClientDo { client =>
      assert(client.describe_cluster_name() === "Test Cluster0")
    }
  }
}
