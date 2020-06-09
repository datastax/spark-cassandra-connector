package com.datastax.spark.connector.rdd.partitioner

import java.net.{InetAddress, InetSocketAddress}

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.{Partition, Partitioner}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag


case class ReplicaPartition(index: Int, endpoints: Array[String]) extends EndpointPartition

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
  @transient private val rwf: RowWriterFactory[T]) extends Partitioner {

  val _keyspace = CqlIdentifier.fromInternal(keyspace) // TODO Fix this

  val tableDef = tableFromCassandra(connector, keyspace, table)
  val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
    tableDef,
    partitionKeyMapper.selectFrom(tableDef)
  )

  @transient lazy private val tokenGenerator = new TokenGenerator[T](connector, tableDef, rowWriter)
  @transient lazy private val tokenMap = connector.withSessionDo(_.getMetadata.getTokenMap.get)//TODO Handle missing
  @transient lazy private val protocolVersion = connector.withSessionDo(_.getContext.getProtocolVersion)
  @transient lazy private val clazz = implicitly[ClassTag[T]].runtimeClass

  private val hosts = connector.hosts.map(_.getAddress).toVector
  private val hostSet = hosts.toSet
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
        val token = tokenGenerator.getTokenFor(key)
        val tokenHash = Math.abs(token.hashCode())
        val replicas = tokenMap
          .getReplicas(_keyspace, token)
          .map(n => DriverUtil.toAddress(n).get.getAddress)

        val replicaSetInDC = (hostSet & replicas).toVector
        if (replicaSetInDC.nonEmpty) {
          val endpoint = replicaSetInDC(absModulo(tokenHash, replicaSetInDC.size))
          hostMap(endpoint)(absModulo(tokenHash, partitionsPerReplicaSet))
        } else {
          hostMap(randomHost(tokenHash))(absModulo(tokenHash, partitionsPerReplicaSet))
        }
      case _ => throw new IllegalArgumentException(
        "ReplicaPartitioner can only determine the partition of a tuple whose key is a non-empty Set[InetAddress]. " +
          s"Invalid key: $key")
    }
  }

  override def numPartitions: Int = partitionsPerReplicaSet * numHosts

  val nodeAddresses = new NodeAddresses(connector)

  def getEndpointPartition(partition: Partition): ReplicaPartition = {
    val endpoints = indexMap.getOrElse(partition.index,
      throw new RuntimeException(s"$indexMap : Can't get an endpoint for Partition $partition.index"))
    ReplicaPartition(index = partition.index, endpoints = nodeAddresses.hostNames(endpoints).toArray)
  }

}

