package com.datastax.spark.connector.writer

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.BenchmarkUtil
import org.apache.spark.{SparkConf, SparkContext}

object BasicWriteBenchmark extends App {

  val cassandraPartitionsToWrite = 100000
  val rowsPerCassandraPartition = 10
  val rowsPerSparkPartition = 50000
  
  val conf = new SparkConf(true)
    .setAppName("Write performance test")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.cassandra.output.concurrent.writes", "16")
    .set("spark.cassandra.output.batch.size.bytes", "4096")
    .set("spark.cassandra.output.batch.grouping.key", "partition")
    .setMaster("local[1]")

  val sc = new SparkContext(conf)
  val conn = CassandraConnector(conf)

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS benchmarks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS benchmarks.write_benchmark (p INT, c INT, value VARCHAR, PRIMARY KEY(p, c))")
  }

  BenchmarkUtil.printTime {
    val col =
      for (i <- 1 to cassandraPartitionsToWrite; j <- 1 to rowsPerCassandraPartition)
      yield (i, j, "data:" + i + ":" + j)
    val rdd = sc.parallelize(col, cassandraPartitionsToWrite * rowsPerCassandraPartition / rowsPerSparkPartition)
    rdd.saveToCassandra("benchmarks", "write_benchmark")
  }

  sc.stop()
}
