/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import org.junit.runner.RunWith
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import com.datastax.bdp.transport.client.HadoopBasedClientConfiguration
import com.datastax.driver.dse.DseSession
import com.datastax.spark.connector.DseConfiguration._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.rdd.ReadConf

@RunWith(classOf[JUnitRunner])
class DseCassandraConnectionFactorySpec extends DseITFlatSpecBase with Matchers {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(sparkConf)

  override lazy val conn = CassandraConnector(sparkConf)

  override val ks = "dsecassconnfact"
  val table = "atab"

  beforeClass {
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
    sc.cassandraTable(ks, table).connector.withClusterDo(cluster =>
      DseCassandraConnectionFactory.continuousPagingEnabled(cluster) should be(true)
    )
  }

  it should " make DseSession capable sessions" in {
    conn.withSessionDo { session =>
      val dseSession = session.asInstanceOf[DseSession]
    }
  }

  it should "be able to read a table" in {
    val row = sc.cassandraTable[(Int, Int, Int)](ks, table)
      .withReadConf(ReadConf(Some(1))).collect.head
    row should be(1, 1, 1)
  }
}
