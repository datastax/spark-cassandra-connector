package com.datastax.spark.connector.rdd

import org.apache.cassandra.tools.NodeProbe
import org.scalatest.Inspectors
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{CassandraRunner, EmbeddedCassandra, YamlTransformations}
import com.datastax.spark.connector.rdd.partitioner.DataSizeEstimates
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory

class CassandraTableScanRDDSpec extends SparkCassandraITFlatSpecBase with Inspectors {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)
  val tokenFactory = TokenFactory.forSystemLocalPartitioner(conn)
  val tableName = "data"
  val noMinimalThreshold = Int.MinValue

  "CassandraTableScanRDD" should "favor user provided split count over minimal threshold" in {
    val userProvidedSplitCount = 8
    val minimalSplitCountThreshold = 32
    val rddWith64MB = getCassandraTableScanRDD(splitSizeMB = 1, splitCount = Some(userProvidedSplitCount),
      minimalSplitCountThreshold = minimalSplitCountThreshold)

    val partitions = rddWith64MB.getPartitions

    partitions.length should be(userProvidedSplitCount +- 1)
  }

  it should "favor user provided split count over size-estimated partitions" in {
    val userProvidedSplitCount = 8
    val rddWith64MB = getCassandraTableScanRDD(splitSizeMB = 1, splitCount = Some(userProvidedSplitCount),
      minimalSplitCountThreshold = noMinimalThreshold)

    val partitions = rddWith64MB.getPartitions

    partitions.length should be(userProvidedSplitCount +- 1)
  }

  it should "create size-estimated partitions with splitSize size" in {
    val rddWith64MB = getCassandraTableScanRDD(splitSizeMB = 1, minimalSplitCountThreshold = noMinimalThreshold)

    val partitions = rddWith64MB.getPartitions

    // theoretically there should be 64 splits, but it is ok to be "a little" inaccurate
    partitions.length should (be >= 16 and be <= 256)
  }

  it should "create size-estimated partitions when above minimal threshold" in {
    val minimalSplitCountThreshold = 2
    val rddWith64MB = getCassandraTableScanRDD(splitSizeMB = 1, minimalSplitCountThreshold = minimalSplitCountThreshold)

    val partitions = rddWith64MB.getPartitions

    // theoretically there should be 64 splits, but it is ok to be "a little" inaccurate
    partitions.length should (be >= 16 and be <= 256)
  }

  it should "create size-estimated partitions but not less than minimum partitions threshold" in {
    val minimalSplitCountThreshold = 64
    val rddWith64MB = getCassandraTableScanRDD(splitSizeMB = 32, minimalSplitCountThreshold = minimalSplitCountThreshold)

    val partitions = rddWith64MB.getPartitions

    partitions.length should be >= minimalSplitCountThreshold
  }

  it should "align index fields of partitions with their place in the array" in {
    val minimalSplitCountThreshold = 64
    val rddWith64MB = getCassandraTableScanRDD(splitSizeMB = 32, minimalSplitCountThreshold = minimalSplitCountThreshold)

    val partitions = rddWith64MB.getPartitions

    forAll(partitions.zipWithIndex) { case (part, index) => part.index should be(index) }
  }

  override def beforeAll(): Unit = {
    conn.withSessionDo { session =>

      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $ks " +
        s"WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

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
        EmbeddedCassandra.cassandraRunners.get(0).map(_.baseConfiguration.jmxPort)
          .getOrElse(CassandraRunner.DefaultJmxPort))
      nodeProbe.forceKeyspaceFlush(ks, tableName)
    }

    val timeout = CassandraRunner.SizeEstimatesUpdateIntervalInSeconds * 1000 * 5
    assert(DataSizeEstimates.waitForDataSizeEstimates(conn, ks, tableName, timeout),
      s"Data size estimates not present after $timeout ms. Test cannot be finished.")
  }

  private def getCassandraTableScanRDD(
    splitSizeMB: Int,
    splitCount: Option[Int] = None,
    minimalSplitCountThreshold: Int): CassandraTableScanRDD[AnyRef] = {
    val readConf = new ReadConf(splitSizeInMB = splitSizeMB, splitCount = splitCount)

    new CassandraTableScanRDD[AnyRef](sc, conn, ks, tableName, readConf = readConf) {
      override def minimalSplitCount: Int = minimalSplitCountThreshold
    }
  }
}
