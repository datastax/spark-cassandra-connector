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

  //We aren't going to change the data
  override def compute(split: Partition, context: TaskContext): Iterator[T] = prev.iterator(split, context)

  @transient override val partitioner: Option[Partitioner] = prev.partitioner

  /**
   * This RDD was partitioned using the Replica Partitioner so we can use that to get preferred location data
   */
  override def getPartitions: Array[Partition] = {
    partitioner match {
      case Some(rp: ReplicaPartitioner) => prev.partitions.map(partition => rp.getEndpointPartition(partition))
      case _ => throw new IllegalArgumentException("CassandraPartitionedRDD hasn't been " +
        "partitioned by ReplicaPartitioner. Unable to do any work with data locality.")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case epp: ReplicaPartition =>
      epp.endpoints.flatMap { inet => Seq(inet.getHostAddress, inet.getHostName) }.toSeq.distinct

    case other: Partition => throw new IllegalArgumentException(
      "CassandraPartitionedRDD doesn't have Endpointed Partitions. PrefferedLocations cannot be" +
        "deterimined")
  }
}
