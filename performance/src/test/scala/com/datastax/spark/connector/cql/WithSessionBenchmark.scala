package com.datastax.spark.connector.cql

import java.util.concurrent.TimeUnit

import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.spark.connector.SparkCassandraITSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import org.openjdk.jmh.annotations._

import scala.util.Random

@State(Scope.Thread)
@Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class WithSessionBenchmark extends SparkCassandraITSpecBase with DefaultCluster {

  override lazy val conn: CassandraConnector = CassandraConnector(
    defaultConf.set(CassandraConnectorConf.KeepAliveMillisParam.name, "0"))

  @Benchmark
  def createKeyspace: ResultSet = {
    val keyspace = Random.alphanumeric.take(10).mkString("")
    conn.withSessionDo { s =>
      s.execute(s"""CREATE KEYSPACE IF NOT EXISTS "$keyspace" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
    }
  }

}