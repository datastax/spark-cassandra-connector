package com.datastax.spark.connector.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

/**
 * RDD created by repartitionByCassandraReplica with preferred locations mapping to the CassandraReplicas
 * each partition was created for.
 */
class CassandraPartitionedRDD[T](
    prev: RDD[T],
    keyspace:String,
    table:String)(
  implicit
    ct: ClassTag[T])
  extends RDD[T](prev) {

  // We aren't going to change the data
  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    prev.iterator(split, context)

  @transient
  override val partitioner: Option[Partitioner] = prev.partitioner

  private val replicaPartitioner: ReplicaPartitioner[_] =
    partitioner match {
      case Some(rp: ReplicaPartitioner[_]) => rp
      case other => throw new IllegalArgumentException(
        s"""CassandraPartitionedRDD hasn't been
           |partitioned by ReplicaPartitioner. Unable to do any work with data locality.
           |Found: $other""".stripMargin)
    }

  private lazy val nodeAddresses = new NodeAddresses(replicaPartitioner.connector)

  override def getPartitions: Array[Partition] =
    prev.partitions.map(partition => replicaPartitioner.getEndpointPartition(partition))

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case epp: ReplicaPartition =>
      epp.endpoints.flatMap(nodeAddresses.hostNames).toSeq
    case other: Partition => throw new IllegalArgumentException(
      "CassandraPartitionedRDD doesn't have Endpointed Partitions. PrefferedLocations cannot be" +
        "deterimined")
  }
}
