package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.{Partition, Partitioner}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._


case class ReplicaPartition(index: Int, endpoints: Set[InetAddress]) extends EndpointPartition

/**
 * The replica partitioner will work on an RDD which is keyed on sets of InetAddresses representing Cassandra
 * Hosts . It will group keys which share a common IP address into partitionsPerReplicaSet Partitions.
 * @param partitionsPerReplicaSet The number of Spark Partitions to make Per Unique Endpoint
 */
class ReplicaPartitioner[T](
  table: String,
  keyspace: String,
  partitionsPerReplicaSet: Int,
  partitionKeyMapper: ColumnSelector,
  val connector: CassandraConnector)(
implicit
  currentType: ClassTag[T],
  @transient rwf: RowWriterFactory[T]) extends Partitioner {

  val tableDef = Schema.tableFromCassandra(connector, keyspace, table)
  val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
    tableDef,
    partitionKeyMapper.selectFrom(tableDef)
  )

  @transient lazy private[spark] val tokenGenerator = new TokenGenerator[T](connector, tableDef, rowWriter)
  @transient lazy private val metadata = connector.withClusterDo(_.getMetadata)
  @transient lazy private val protocolVersion = connector
    .withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersion)
  @transient lazy private val clazz = implicitly[ClassTag[T]].runtimeClass

  private val hosts = connector.hosts.toVector
  private val hostSet = connector.hosts
  private val numHosts = hosts.size
  private val partitionIndexes = (0 until partitionsPerReplicaSet * numHosts)
    .grouped(partitionsPerReplicaSet)
    .toList

  private val hostMap = (hosts zip partitionIndexes).toMap
  // Ip1 -> (0,1,2,..), Ip2 -> (11,12,13...)
  private val indexMap = for ((ip, partitions) <- hostMap; partition <- partitions) yield (partition, ip)
  // 0->IP1, 1-> IP1, ...

  private def absModulo(dividend: Int, divisor: Int) : Int = Math.abs(dividend % divisor)

  private def randomHost(index: Int): InetAddress = hosts(absModulo(index, hosts.length))

  /**
   * Given a set of endpoints, pick a random endpoint, and then a random partition owned by that
   * endpoint. If the requested host doesn't exist chose another random host. Only uses valid hosts
   * from the connected datacenter.
   * @param key A Set[InetAddress] of replicas for this Cassandra Partition
   * @return An integer between 0 and numPartitions
   */
  override def getPartition(key: Any): Int = {
    key match {
      case key: T if clazz.isInstance(key) =>
        //Only use ReplicaEndpoints in the connected DC
        val keyBuffer = tokenGenerator.getPartitionKeyBufferFor(key)
        val keyHash = Math.abs(keyBuffer.hashCode())
        val replicas = metadata
          .getReplicas(keyspace, keyBuffer)
          .map(_.getBroadcastAddress)

        val replicaSetInDC = (hostSet & replicas).toVector
        if (replicaSetInDC.nonEmpty) {
          val endpoint = replicaSetInDC(absModulo(keyHash, replicaSetInDC.size))
          hostMap(endpoint)(absModulo(keyHash, partitionsPerReplicaSet))
        } else {
          hostMap(randomHost(keyHash))(absModulo(keyHash, partitionsPerReplicaSet))
        }
      case _ => throw new IllegalArgumentException(
        "ReplicaPartitioner can only determine the partition of a tuple whose key is a non-empty Set[InetAddress]. " +
          s"Invalid key: $key")
    }
  }

  override def numPartitions: Int = partitionsPerReplicaSet * numHosts

  def getEndpointPartition(partition: Partition): ReplicaPartition = {
    val endpoints = indexMap.getOrElse(partition.index,
      throw new RuntimeException(s"$indexMap : Can't get an endpoint for Partition $partition.index"))
    new ReplicaPartition(index = partition.index, endpoints = Set(endpoints))
  }

}

