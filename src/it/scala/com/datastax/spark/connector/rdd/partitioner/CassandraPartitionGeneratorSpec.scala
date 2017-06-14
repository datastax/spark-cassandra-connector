package com.datastax.spark.connector.rdd.partitioner

import com.datastax.driver.core.Row
import org.apache.cassandra.tools.NodeProbe
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, Schema}
import com.datastax.spark.connector.embedded.{CassandraRunner, EmbeddedCassandra, SparkTemplate}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.{Murmur3TokenFactory, RandomPartitionerTokenFactory}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.rdd.{CqlWhereClause, ReadConf}
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import com.datastax.spark.connector.embedded.YamlTransformations


class CassandraPartitionGeneratorSpec
  extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq(YamlTransformations.Default))
  override val conn = CassandraConnector(defaultConf)
  implicit val tokenFactory = TokenFactory.forSystemLocalPartitioner(conn)

  conn.withSessionDo { session =>
    createKeyspace(session)
    session.execute(s"CREATE TABLE $ks.empty(key INT PRIMARY KEY)")
  }

  // TODO: Currently CassandraPartitionGenerator uses a size-based algorithm that doesn't guarantee exact
  // split count, so we are only checking if the split count is "close enough" to the desired value.
  // Should be improved in the future.
  private def testPartitionCount(numPartitions: Int, min: Int, max: Int): Unit = {
    val table = Schema.fromCassandra(conn, Some(ks), Some("empty")).tables.head
    val partitioner = CassandraPartitionGenerator(conn, table, numPartitions)
    val partitions = partitioner.partitions
    partitions.length should be >= min
    partitions.length should be <= max
  }

  "CassandraPartitionGenerator" should "create 1 partition if splitCount == 1" in {
    testPartitionCount(1, 1, 1)
  }

  // we won't run it on a 10000 node cluster, so we don't need to check node count
  it should "create about 10000 partitions when splitCount == 10000" in {
    testPartitionCount(10000, 9000, 11000)
  }
}
