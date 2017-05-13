package com.datastax.spark.connector.rdd.partitioner

import com.datastax.driver.core.Row
import org.apache.cassandra.tools.NodeProbe
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, Schema}
import com.datastax.spark.connector.embedded.{CassandraRunner, EmbeddedCassandra, SparkTemplate}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.{Murmur3TokenFactory, RandomPartitionerTokenFactory}
import com.datastax.spark.connector.rdd.{CqlWhereClause, ReadConf}
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra

import scala.collection.JavaConverters._

class CassandraPartitionGeneratorSpec
  extends SparkCassandraITFlatSpecBase with Inspectors  {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  override val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)
    session.execute(s"CREATE TABLE $ks.empty(key INT PRIMARY KEY)")
    session.execute(s"CREATE TABLE $ks.overflow(key INT PRIMARY KEY)")
  }

  // TODO: Currently CassandraPartitionGenerator uses a size-based algorithm that doesn't guarantee exact
  // split count, so we are only checking if the split count is "close enough" to the desired value.
  // Should be improved in the future.
  private def testPartitionCount(numPartitions: Int, min: Int, max: Int): Unit = {
    val table = Schema.fromCassandra(conn, Some(ks), Some("empty")).tables.head
    val partitioner = CassandraPartitionGenerator(conn, table, Some(numPartitions), 10000)
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
    val partitioner = CassandraPartitionGenerator(conn, table, splitCount = None, splitSize = 1000000)
    val partitions = partitioner.partitions

    // theoretically there should be 64 splits, but it is ok to be "a little" inaccurate
    partitions.length should be >= 16
    partitions.length should be <= 256
  }

  it should "align index fields of partitions with their place in the array" in {
    val table = Schema.fromCassandra(conn, Some(ks), Some("data")).tables.head
    val partitioner = CassandraPartitionGenerator(conn, table, splitCount = Some(1000), splitSize = 100)
    val partToIndex = partitioner.partitions.zipWithIndex
    forAll (partToIndex) { case (part, index) => part.index should be (index) }
  }

  it should "not fail if the sizeEstimates overflow" in {
    val table = Schema.fromCassandra(conn, Some(ks), Some("overflow")).tables.head

    fudgeSizeEstimatesTable("overflow", Long.MaxValue)
    val partitions = CassandraPartitionGenerator(
      conn,
      table,
      splitCount = None,
      splitSize = ReadConf.SplitSizeInMBParam.default * 1024 * 1024).partitions

    partitions.size should be >= 1
    partitions.size should be <= conn.withClusterDo(_.getMetadata.getAllHosts.size * 2 + 5)
  }

  it should "fail fast if splitSize is 0" in {
    val table = Schema.fromCassandra(conn, Some(ks), Some("overflow")).tables.head
    intercept[IllegalArgumentException]{
      CassandraPartitionGenerator(
      conn,
      table,
      splitCount = None,
      splitSize = 0)
    }
  }


  /**
    * Fudges the size estimates information for the given table
    * Attempts to replace all records for existing ranges with a single record
    * giving a mean size of sizeFudgeInMB
    */
  def fudgeSizeEstimatesTable(tableName: String, sizeFudgeInMB: Long) = {

    val meta = conn.withClusterDo(_.getMetadata)
    val tokenFactory = meta.getPartitioner match {
      case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitionerTokenFactory
      case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3TokenFactory
      case x => throw new IllegalArgumentException(x)
    }

    conn.withSessionDo { case session =>
      session.execute(
        """DELETE FROM system.size_estimates
          |where keyspace_name = ?
          |AND table_name = ?""".stripMargin, ks, "overflow")

      session.execute(
        """
          |INSERT INTO system.size_estimates (
          |  keyspace_name,
          |  table_name,
          |  range_start,
          |  range_end,
          |  mean_partition_size,
          |  partitions_count)
          |  VALUES (?,?,?,?,?,?)
        """.
          stripMargin,
        ks,
        tableName,
        tokenFactory.minToken.toString,
        tokenFactory.maxToken.toString,
        sizeFudgeInMB * 1024 * 1024: java.lang.Long,
        1L: java.lang.Long)
      }
    }
}
