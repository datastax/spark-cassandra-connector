/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import com.datastax.dse.driver.api.core.DseSession
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession
import com.datastax.dse.driver.api.core.graph.GraphSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.AuthCluster
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually

class DseCassandraConnectionFactorySpec extends SparkCassandraITFlatSpecBase with AuthCluster with Matchers with Eventually {

  override lazy val conn = new CassandraConnector(CassandraConnectorConf(sparkConf).copy(
    connectionFactory = DseCassandraConnectionFactory
  ))

  override val ks = "dsecassconnfact"
  val table = "atab"

  override def beforeClass {
    conn.withSessionDo { case session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      session.execute(s"CREATE TABLE IF NOT EXISTS $ks.$table (p int, c int, d int, PRIMARY KEY (p,c))")
      session.execute(s"INSERT INTO $ks.$table (p,c,d) VALUES (1,1,1)")
    }
  }

  "DseCassandraConnectionFactory" should "have paging on by default" in {
    val session = sc.cassandraTable(ks, table).connector.withSessionDo(session => session)
    DseCassandraConnectionFactory.continuousPagingEnabled(session) should be(true)
  }

  it should "make DseSession capable sessions" in {
    conn.withSessionDo { session =>
      session.asInstanceOf[ContinuousSession]
      session.asInstanceOf[GraphSession]
      session.asInstanceOf[DseSession]
    }
  }

  it should "be able to read a table" in {
    val row = sc.cassandraTable[(Int, Int, Int)](ks, table)
      .withReadConf(ReadConf(Some(1))).collect.head
    row should be(1, 1, 1)
  }

  it should "use cached dse session" in {
    val zeroKeepAliveConnector = new CassandraConnector(conn.conf.copy(keepAliveMillis = 0))

    val session1 = zeroKeepAliveConnector.openSession()
    val session2 = zeroKeepAliveConnector.openSession()

    session1.close()
    session1.isClosed shouldBe false
    session2.isClosed shouldBe false

    session1.close()
    session1.isClosed shouldBe false
    session2.isClosed shouldBe false

    session2.close()
    session1.isClosed shouldBe true
    session2.isClosed shouldBe true
  }

  it should "use provided local dc" in {
    val connectorWithDc = new CassandraConnector(conn.conf.copy(localDC = Some("Cassandra")))

    connectorWithDc.withSessionDo { session =>
      session.getContext.getConfig.getDefaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER) shouldBe "Cassandra"
    }
  }
}
