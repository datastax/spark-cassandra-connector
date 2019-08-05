package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.SSLCluster

class CassandraSSLClientAuthConnectorSpec extends SparkCassandraITFlatSpecBase with SSLCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  "A CassandraConnector" should "be able to use a secure connection when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
    }
  }

}
