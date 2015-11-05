package com.datastax.spark.connector.cql

import com.datastax.spark.connector.embedded.SparkTemplate._
import org.apache.spark.SparkConf

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.embedded.EmbeddedCassandra

class CassandraAuthenticatedConnectorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-password-auth.yaml.template"))

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  val conf = defaultConf
  conf.set(DefaultAuthConfFactory.CassandraUserNameProperty, "cassandra")
  conf.set(DefaultAuthConfFactory.CassandraPasswordProperty, "cassandra")

  "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName != null)
    }
  }

  it should "pick up user and password from SparkConf" in {
    val host = EmbeddedCassandra.getHost(0).getHostAddress
    val conf = defaultConf
      .set(CassandraConnectorConf.ConnectionHostParam.name, host)
      .set(DefaultAuthConfFactory.CassandraUserNameProperty, "cassandra")
      .set(DefaultAuthConfFactory.CassandraPasswordProperty, "cassandra")

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }
}
