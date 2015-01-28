package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{Partition, Partitioner}


case class ReplicaPartition(index: Int, endpoints: Set[InetAddress]) extends EndpointPartition

/**
 * The replica partitioner will work on an RDD which is keyed on sets of InetAdresses representing Cassandra
 * Hosts . It will group keys which share a common IP address into partitionsPerReplicaSet Partitions.
 * @param partitionsPerReplicaSet The number of Spark Partitions to make Per Unique Endpoint
 * @param connector
 */
class ReplicaPartitioner(partitionsPerReplicaSet: Int, connector: CassandraConnector) extends Partitioner {
  /* TODO We Need JAVA-312 to get sets of replicas instead of single endpoints. Once we have that we'll be able to
  build a map of Set[ip,ip,...] => Index before looking at our data and give the all options for the preferred location
   for a partition*/
  val hosts = connector.hosts.toVector
  val numHosts = hosts.size
  val partitionIndexes = (0 until partitionsPerReplicaSet * numHosts).grouped(partitionsPerReplicaSet).toList
  val hostMap = (hosts zip partitionIndexes).toMap
  // Ip1 -> (0,1,2,..), Ip2 -> (11,12,13...)
  val indexMap = for ((ip, partitions) <- hostMap; partition <- partitions) yield (partition, ip)
  // 0->IP1, 1-> IP1, ...
  val rand = new java.util.Random()

  /**
   * Given a set of endpoints, pick a random endpoint, and then a random partition owned by that endpoint. If the
   * requested host doesn't exist chose another random host.
   * @param key A Set[InetAddress] of replicas for this Cassandra Partition
   * @return An integer between 0 and numPartitions
   */
  override def getPartition(key: Any): Int = {
    key match {
      case key: Set[InetAddress] => {
        val replicaSet = key.asInstanceOf[Set[InetAddress]].toVector
        val endpoint = replicaSet(rand.nextInt(replicaSet.size))
        hostMap.getOrElse(endpoint, hostMap(hosts(rand.nextInt(numHosts))))(rand.nextInt(partitionsPerReplicaSet))
      }
      case _ => throw new IllegalArgumentException("ReplicaPartitioner can only determine the partition of tuples whose keys that are of type Set[InetAddress]")
    }
  }

  override def numPartitions: Int = partitionsPerReplicaSet * numHosts

  def getEndpointPartition(partition: Partition): ReplicaPartition = {
    val endpoints = indexMap.getOrElse(partition.index,
      throw new RuntimeException(s"${indexMap} : Can't get an endpoint for Partition $partition.index"))
    new ReplicaPartition(index = partition.index, endpoints = Set(endpoints))
  }

}

