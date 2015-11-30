package com.datastax.spark.connector.cql

import com.datastax.spark.connector.embedded.SparkTemplate._

import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase

class CassandraAuthenticatedConnectorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-password-auth.yaml.template"))

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  val conf = defaultConf
  conf.set(DefaultAuthConfFactory.UserNameParam.name, "cassandra")
  conf.set(DefaultAuthConfFactory.PasswordParam.name, "cassandra")

  "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName != null)
    }
  }

  it should "pick up user and password from SparkConf" in {
    val conf = defaultConf
      .set(DefaultAuthConfFactory.UserNameParam.name, "cassandra")
      .set(DefaultAuthConfFactory.PasswordParam.name, "cassandra")

    // would throw exception if connection unsuccessful
    val conn2 = CassandraConnector(conf)
    conn2.withSessionDo { session => }
  }
}
