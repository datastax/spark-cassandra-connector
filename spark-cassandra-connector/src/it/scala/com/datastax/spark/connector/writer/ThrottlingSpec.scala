package com.datastax.spark.connector.writer

import com.datastax.spark.connector._
import com.datastax.spark.connector.{SomeColumns, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations

import scala.concurrent.Future

class ThrottlingSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)




  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute( s"""CREATE TABLE $ks.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))""")
      })
  }

  "Throttling" should "prevent failures based on driver max queue size while writing" in {
      conn.withClusterDo{cluster =>
        val poolingOptions = cluster.getConfiguration.getPoolingOptions
        poolingOptions.setMaxQueueSize(1)
        poolingOptions.setMaxConnectionsPerHost(com.datastax.driver.core.HostDistance.LOCAL, 1)
      }
      val rows = (1 to 10000).map(x => (x, x.toLong, x.toString))
      sc.parallelize(rows).saveToCassandra(ks, "key_value")
      sc.cassandraTable(ks, "key_value").cassandraCount() should be(10000)
  }

  it should "prevent failures based on driver pooling limits while joining" in {
    conn.withClusterDo{cluster =>
      val poolingOptions = cluster.getConfiguration.getPoolingOptions
      poolingOptions.setMaxQueueSize(1)
      poolingOptions.setMaxConnectionsPerHost(com.datastax.driver.core.HostDistance.LOCAL, 1)
    }

    val rows = (1 to 100000).map(Tuple1(_))
    val results = sc.parallelize(rows).joinWithCassandraTable(ks, "key_value")
    results.count should be(10000)
  }
}
