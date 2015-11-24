package com.datastax.spark.connector.rdd.partitioner

import org.apache.cassandra.tools.NodeProbe
import org.scalatest.{Matchers, FlatSpec}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}
import com.datastax.spark.connector.embedded.{CassandraRunner, SparkTemplate, EmbeddedCassandra}
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra

class CassandraRDDPartitionerSpec
  extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)
    session.execute(s"CREATE TABLE $ks.empty(key INT PRIMARY KEY)")
  }

  // TODO: Currently CassandraRDDPartitioner uses a size-based algorithm that doesn't guarantee exact
  // split count, so we are only checking if the split count is "close enough" to the desired value.
  // Should be improved in the future.
  private def testPartitionCount(numPartitions: Int, min: Int, max: Int): Unit = {
    val table = Schema.fromCassandra(conn, Some(ks), Some("empty")).tables.head
    val partitioner = CassandraRDDPartitioner(conn, table, Some(numPartitions), 10000)
    val partitions = partitioner.partitions(CqlWhereClause.empty)
    partitions.length should be >= min
    partitions.length should be <= max
  }

  "CassandraRDDPartitioner" should "create 1 partition per node if splitCount == 1" in {
    testPartitionCount(1, conn.hosts.size, conn.hosts.size)
  }

  // we won't run it on a 10000 node cluster, so we don't need to check node count
  it should "create about 10000 partitions when splitCount == 10000" in {
    testPartitionCount(10000, 9000, 11000)
  }

  it should "create multiple partitions if the amount of data is big enough" in {
    val tableName = "data"
    conn.withSessionDo { session =>
      session.execute(s"CREATE TABLE $ks.$tableName(key int primary key, value text)")
      val st = session.prepare(s"INSERT INTO $ks.$tableName(key, value) VALUES(?, ?)")
      // 1M rows x 64 bytes of payload = 64 MB of data + overhead
      for (i <- (1 to 1000000).par) {
        val key = i.asInstanceOf[AnyRef]
        val value = "123456789.123456789.123456789.123456789.123456789.123456789."
        session.execute(st.bind(key, value))
      }
    }

    for (host <- conn.hosts) {
      val nodeProbe = new NodeProbe(host.getHostAddress,
        EmbeddedCassandra.cassandraRunners(0).map(_.jmxPort).getOrElse(CassandraRunner.DefaultJmxPort))
      nodeProbe.forceKeyspaceFlush(ks, tableName)
    }

    val timeout = CassandraRunner.SizeEstimatesUpdateIntervalInSeconds * 1000 * 5
    assert(DataSizeEstimates.waitForDataSizeEstimates(conn, ks, tableName, timeout),
      s"Data size estimates not present after $timeout ms. Test cannot be finished.")

    val table = Schema.fromCassandra(conn, Some(ks), Some(tableName)).tables.head
    val partitioner = CassandraRDDPartitioner(conn, table, splitCount = None, splitSize = 1000000)
    val partitions = partitioner.partitions(CqlWhereClause.empty)

    // theoretically there should be 64 splits, but it is ok to be "a little" inaccurate
    partitions.length should be >= 16
    partitions.length should be <= 256
  }

}
