/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession
import com.datastax.dse.driver.api.core.graph.GraphSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ReadConf
import org.scalatest.Matchers

class DseCassandraConnectionFactorySpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Matchers {

  override lazy val conn = CassandraConnector(sparkConf)

  override val ks = "dsecassconnfact"
  val table = "atab"

  override def beforeClass {
    conn.withSessionDo { case session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      session.execute(s"CREATE TABLE $ks.$table (p int, c int, d int, PRIMARY KEY (p,c))")
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
    }
  }

  it should "be able to read a table" in {
    val row = sc.cassandraTable[(Int, Int, Int)](ks, table)
      .withReadConf(ReadConf(Some(1))).collect.head
    row should be(1, 1, 1)
  }
}
