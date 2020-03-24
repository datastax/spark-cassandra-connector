package com.datastax.spark.connector.util

import java.util.concurrent.CompletableFuture

import com.datastax.spark.connector.cluster.DefaultCluster

import scala.language.postfixOps
import org.scalatest.concurrent.Waiters
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}

class MultiThreadedSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Waiters {

  override lazy val conn = CassandraConnector(defaultConf)
  val count = 1000

  val tab = "mt_test"

  conn.withSessionDo { session =>
    createKeyspace(session)
    val executor = getExecutor(session)
    session.execute(s"CREATE TABLE $ks.$tab (pkey int PRIMARY KEY, value varchar)")
    val ps = session.prepare(s"INSERT INTO $ks.$tab (pkey, value) VALUES (?, ?)")

    awaitAll(
      for (i <- 1 to count) yield
        executor.executeAsync(ps.bind(i: java.lang.Integer, "value " + i))
    )
    executor.waitForCurrentlyExecutingTasks()
  }

  "A Spark Context " should " be able to read a Cassandra table in different threads" in {

    val w = new Waiter

    val threads = for (theadnum <- 1 to 5) yield new Thread(new Runnable {
      def run() {
        val rdd = sc.cassandraTable[(Int, String)](ks, tab)
        val result = rdd.collect
        w {
          result should have size count
        }
        w.dismiss()
      }
    })
    for (thread <- threads) thread.start()
    import org.scalatest.time.SpanSugar._

    w.await(timeout(90 seconds), dismissals(5))
    for (thread <- threads) thread.join()
  }

}
