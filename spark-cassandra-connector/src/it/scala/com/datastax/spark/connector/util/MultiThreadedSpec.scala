package com.datastax.spark.connector.util

import scala.language.postfixOps

import org.scalatest.concurrent.AsyncAssertions

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}


class MultiThreadedSpec extends SparkCassandraITFlatSpecBase with AsyncAssertions {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf)

  val conn = CassandraConnector(defaultConf)
  val count = 1000

  val tab = "mt_test"

  conn.withSessionDo { session =>
    createKeyspace(session)
    session.execute(s"CREATE TABLE $ks.$tab (pkey int PRIMARY KEY, value varchar)")

    (for (i <- 1 to count) yield
      session.executeAsync(s"INSERT INTO $ks.$tab (pkey, value) VALUES (?, ?)",
        i: java.lang.Integer,
        "value " + i)
      ).par.foreach(_.getUninterruptibly)
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

    w.await(timeout(30 seconds), dismissals(5))
    for (thread <- threads) thread.join()
  }

}
