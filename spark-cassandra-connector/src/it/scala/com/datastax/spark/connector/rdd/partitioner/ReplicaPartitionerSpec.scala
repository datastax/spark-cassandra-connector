package com.datastax.spark.connector.rdd.partitioner

import java.net.{Inet4Address, Inet6Address, InetAddress}
import java.nio.file.Files
import java.nio.file.attribute.FileAttribute

import com.datastax.spark.connector.{SomeColumns, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, MultipleRetryPolicy}
import com.datastax.driver.core.TokenRange
import com.datastax.driver.core.policies.{RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.spark.connector.embedded.{CassandraRunner, YamlTransformations}
import com.datastax.spark.connector.embedded.EmbeddedCassandra.cassandraPorts
import com.datastax.spark.connector.embedded.YamlTransformations.CassandraConfiguration

import scala.collection.JavaConversions._
import org.scalatest.{BeforeAndAfterAll, Inspectors}
import org.scalatest.concurrent.Eventually

class ReplicaPartitionerSpec
  extends SparkCassandraITFlatSpecBase with Inspectors with Eventually with BeforeAndAfterAll{

  val min = 1
  val max = 100

  val node1Conf = CassandraConfiguration(
    seeds = List("127.0.0.2"),
    storagePort = 7000,
    sslStoragePort = 7001,
    nativeTransportPort = 9042,
    jmxPort = 30000,
    rpcAddress = "127.0.0.2",
    listenAddress = "127.0.0.2",
    clusterName = "ReplicaPartitionerSpec",
    cassandraDir = Files.createTempDirectory("cdata").toString
  )

  val node2Conf = CassandraConfiguration(
    seeds = List("127.0.0.2"),
    storagePort = 7000,
    sslStoragePort = 7001,
    nativeTransportPort = 9042,
    jmxPort = 30001,
    rpcAddress = "127.0.0.3",
    listenAddress = "127.0.0.3",
    clusterName = "ReplicaPartitionerSpec",
    cassandraDir = Files.createTempDirectory("cdata").toString
  )

  private var nodes: Seq[CassandraRunner] = Seq.empty
  val nodeConfs = Seq(node1Conf, node2Conf)

  override val conn =
    CassandraConnector(defaultConf
      .set(CassandraConnectorConf.ConnectionHostParam.name, "127.0.0.2")
      .set(CassandraConnectorConf.ConnectionPortParam.name, "9042")
    )

  override def beforeAll: Unit = {
    nodes = nodeConfs.map(conf => new CassandraRunner(YamlTransformations.Default, conf))
    eventually {
      conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(s"CREATE TABLE $ks.kv (key INT PRIMARY KEY, value INT)")
        (min to max)
          .map(x => (x: java.lang.Integer, x: java.lang.Integer))
          .map { case (x, y) =>
            session.executeAsync(s"INSERT INTO $ks.kv (key, value) VALUES (?, ?)", x, y) }
          .foreach(_.get)
      }
    }
  }


  "ReplicaPartitoner" should "should use token generator to get correct replica mapping" in conn.withClusterDo { cluster =>
    val metadata = cluster.getMetadata
    val allHosts = metadata.getAllHosts

    val replicaPartitioner = new ReplicaPartitioner[(Int, Int)]("kv", ks, 1, SomeColumns("key"), conn)
    val tokenGenerator = replicaPartitioner.tokenGenerator

    val tap = new TokenAwarePolicy(new RoundRobinPolicy)
    tap.init(cluster, allHosts)


    val rows = (min to max).map( x => (x, x))

    val bs = conn.withSessionDo(_.prepare(s"INSERT INTO $ks.kv (key, value) VALUES (?, ?)"))

    for (row <- rows) {
      val replicas = metadata.getReplicas(ks, tokenGenerator.getPartitionKeyBufferFor(row))
      val hosts = tap.newQueryPlan(ks, bs.bind(row._1: java.lang.Integer, row._2: java.lang.Integer))
      val firstHost = hosts.next
      firstHost should be (replicas.head)
    }
  }

  it should "partition our data into one partition per node of roughly equal size" in {
    val rows = (min to max).map( x => (x, x))
    val replicaPartitioner = new ReplicaPartitioner[(Int, Int)]("kv", ks, 1, SomeColumns("key"), conn)

    val partitionedData = rows.groupBy(replicaPartitioner.getPartition(_))
    partitionedData should have size (2) // There should be two partitions
    partitionedData.foreach{ case (partition, contents) => contents.size should be > (max / 3).toInt} // Each should have roughly half the data
  }

  override def afterAll: Unit = {
    CassandraConnector.evictCache()
    nodes.map(_.destroy())
  }

}
