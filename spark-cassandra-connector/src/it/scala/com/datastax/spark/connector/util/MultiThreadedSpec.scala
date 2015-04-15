package com.datastax.spark.connector.util

import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.scalatest.concurrent.AsyncAssertions


class MultiThreadedSpec extends SparkCassandraITFlatSpecBase with AsyncAssertions{

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  val count = 10000

  val ks = "multi_threaded"
  val tab = "mt_test"

  conn.withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $ks WITH replication = {'class': " +
      "'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE $ks.$tab (pkey int PRIMARY KEY, value varchar)")

    for (i <- 1 to count) {
      session.execute(
        s"INSERT INTO $ks.$tab (pkey, value) VALUES (?, ?)",
        i: java.lang.Integer, "value " + i)
    }
  }

  "A Spark Context " should " be able to read a Cassandra table in different threads" in {

    val w = new Waiter

    val threads = for (theadnum <- 1 to 10) yield new Thread(new Runnable {
      def run() {
        val rdd = sc.cassandraTable[(Int, String)](ks, tab)
        val result = rdd.collect
        w { result should have size (count) }
        w.dismiss()
        }
    })
    for (thread <- threads) thread.start()
    import org.scalatest.time.SpanSugar._

    w.await(timeout(10 seconds), dismissals(10))
    for (thread <- threads) thread.join()
    }

}
