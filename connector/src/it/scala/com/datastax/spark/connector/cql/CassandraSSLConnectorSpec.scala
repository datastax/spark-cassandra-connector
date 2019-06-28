package com.datastax.spark.connector.cql

import com.datastax.spark.connector.CCMTraits.{CCMTrait, SSL}
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase}

class CassandraSSLConnectorSpec extends SparkCassandraITFlatSpecBase {
  override lazy val traits: Set[CCMTrait] = Set(SSL())

  override def conn = CassandraConnector(defaultConf)

  "A CassandraConnector" should "be able to use a secure connection when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName === ccmBridge.getClusterName)
    }
  }

}
