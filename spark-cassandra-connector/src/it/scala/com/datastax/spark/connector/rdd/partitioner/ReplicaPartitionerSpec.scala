package com.datastax.spark.connector.rdd.partitioner

import java.net.{Inet4Address, Inet6Address, InetAddress}

import com.datastax.spark.connector.{SomeColumns, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, MultipleRetryPolicy}
import com.datastax.driver.core.TokenRange
import com.datastax.driver.core.policies.{RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.spark.connector.embedded.CassandraRunner
import com.datastax.spark.connector.embedded.EmbeddedCassandra.cassandraPorts

import scala.collection.JavaConversions._
import org.scalatest.{BeforeAndAfterAll, Inspectors}
import org.scalatest.concurrent.Eventually

class ReplicaPartitionerSpec
  extends SparkCassandraITFlatSpecBase with Inspectors with Eventually with BeforeAndAfterAll{

  val min = 1
  val max = 100

  var runners: Seq[CassandraRunner] = Seq.empty

  override val conn =
    CassandraConnector(defaultConf
      .set(CassandraConnectorConf.ConnectionHostParam.name, "127.0.0.2")
      .set(CassandraConnectorConf.ConnectionPortParam.name, "9042")
    )

  override def beforeAll: Unit = {
    val node1Conf = Map(
      "seeds" -> "127.0.0.2",
      "storage_port" -> "7000",
      "ssl_storage_port" -> "7001",
      "native_transport_port" -> "9042",
      "jmx_port" -> "30000",
      "rpc_address" -> s"127.0.0.2",
      "listen_address" -> s"127.0.0.2",
      "cluster_name" -> "ReplicaPartitionerSpec",
      "keystore_path" -> ClassLoader.getSystemResource("keystore").getPath,
      "truststore_path" -> ClassLoader.getSystemResource("truststore").getPath
    )

    val node2Conf = Map(
      "seeds" -> "127.0.0.2",
      "storage_port" -> "7000",
      "ssl_storage_port" -> "7001",
      "native_transport_port" -> "9042",
      "jmx_port" -> "30001",
      "rpc_address" -> s"127.0.0.3",
      "listen_address" -> s"127.0.0.3",
      "cluster_name" -> "ReplicaPartitionerSpec",
      "keystore_path" -> ClassLoader.getSystemResource("keystore").getPath,
      "truststore_path" -> ClassLoader.getSystemResource("truststore").getPath
    )

    val versionedTemplate = getVersionedConfigTemplate("cassandra-default.yaml.template")

    runners = Seq(
      new CassandraRunner(configTemplate = versionedTemplate, props = node1Conf),
      new CassandraRunner(configTemplate = versionedTemplate, props = node2Conf))

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

  override def afterAll() = {
    runners.foreach(_.destroy)
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

}
