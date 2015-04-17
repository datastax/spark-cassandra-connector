package com.datastax.spark.connector.rdd.partitioner

import org.scalatest.{Matchers, FlatSpec}

import com.datastax.spark.connector.cql.{Schema, CassandraConnector}
import com.datastax.spark.connector.embedded.{SparkTemplate, EmbeddedCassandra}
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra

class CassandraRDDPartitionerSpec
  extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(hosts = Set(EmbeddedCassandra.getHost(0)))
  val keyspaceName = "partitioner_test"
  conn.withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspaceName " +
      s"WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspaceName.empty(key INT PRIMARY KEY)")
  }

  // TODO: Currently CassandraRDDPartitioner uses a size-based algorithm that doesn't guarantee exact
  // split count, so we are only checking if the split count is "close enough" to the desired value.
  // Should be improved in the future.
  private def testPartitionCount(numPartitions: Int, min: Int, max: Int): Unit = {
    val table = Schema.fromCassandra(conn, Some(keyspaceName), Some("empty")).tables.head
    val partitioner = CassandraRDDPartitioner(conn, table, Some(numPartitions), 10000)
    val partitions = partitioner.partitions(CqlWhereClause.empty)
    partitions.length should be >= min
    partitions.length should be <= max
  }

  "CassandraRDDPartitioner" should "create 1 partition if splitCount == 1" in {
    testPartitionCount(1, 1, 1)
  }

  it should "create about 16 partitions when splitCount == 16" in {
    testPartitionCount(16, 13, 20)
  }

  it should "create about 10000 partitions when splitCount == 10000" in {
    testPartitionCount(10000, 9000, 11000)
  }

  // TODO: Add a test for automatic split count tuning (needs Cassandra 2.1.5)

}
