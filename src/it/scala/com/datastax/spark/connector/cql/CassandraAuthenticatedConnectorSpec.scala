package com.datastax.spark.connector.cql

import java.net.InetAddress

import com.datastax.spark.connector.util.CassandraServer
import org.scalatest.{Matchers, FlatSpec}

class CassandraAuthenticatedConnectorSpec  extends FlatSpec with Matchers with CassandraServer {

  useCassandraConfig("cassandra-password-auth.yaml.template")
  val conn = CassandraConnector(InetAddress.getByName("127.0.0.1"), authConf = PasswordAuthConf("cassandra", "cassandra"))

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session.isClosed === false)
      assert(session !== null)
    }
  }

  it should "authenticate with username and password when using thrift" in {
    conn.withCassandraClientDo { client =>
      assert(client.describe_cluster_name() === "Test Cluster")
    }
  }
}
