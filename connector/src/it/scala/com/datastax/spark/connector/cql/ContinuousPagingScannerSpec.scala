/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.cql

import com.datastax.bdp.spark.ContinuousPagingScanner
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.rdd.ReadConf
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future

class ContinuousPagingScannerSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually {

  sparkConf.set(CassandraConnectionFactory.continuousPagingParam.name, "true")

  override lazy val conn = CassandraConnector(sparkConf)

  override val ks = "continuous_paging"
  val table = "atab"

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(s"""CREATE TABLE $ks.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)""")
          session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)""")
        }
      )

      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      session.execute(s"CREATE TABLE $ks.$table (p int, c int, d int, PRIMARY KEY (p,c))")
      session.execute(s"INSERT INTO $ks.$table (p,c,d) VALUES (1,1,1)")
    }
  }

  "ContinuousPagingScanner" should "re-use a session in the same thread" in {
    val sessions = for (x <- 1 to 10) yield {
      val cps = ContinuousPagingScanner(ReadConf(), conn.conf, IndexedSeq.empty)
      cps.close()
      cps.getSession()
    }
    sessions.forall(session => session == sessions(0))
  }

  it should "use a different session than the one provided by the default connector" in {
    val scanner = ContinuousPagingScanner(ReadConf(), conn.conf, IndexedSeq.empty)
    scanner.close()
    conn.withSessionDo(session => session shouldNot be(scanner.getSession()))
  }

  it should "use a single CP session for all threads" in {
    CassandraConnector.evictCache()
    eventually {CassandraConnector.sessionCache.cache.isEmpty}
    val rdd = sc.cassandraTable(ks, table).withReadConf(ReadConf(splitCount = Some(400)))
    rdd.partitions.length should be > 100 //Sanity check that we will have to reuse sessions
    rdd.count
    val sessions = CassandraConnector
      .sessionCache
      .cache
      .keys

    withClue(sessions.map(_.toString).mkString("\n"))(sessions.size should be(1))
  }
}


