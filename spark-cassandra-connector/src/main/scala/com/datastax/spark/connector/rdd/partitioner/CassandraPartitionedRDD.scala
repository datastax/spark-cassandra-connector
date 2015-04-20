package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.collection.JavaConversions._
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
        "partitioned by ReplicaPartitioner. This should be impossible")
    }
  }

  /**
   * This method currently uses thrift to determine the local endpoint and rpc endpoints
   * from whatever endpoint the partition belongs to. This gives us a better chance of matching
   * the bound interface of the Spark Executor. In addition we will add the HostName and
   * HostAddress that we get for each of these endpoints to cover all of the logical choices.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val connector = CassandraConnector(prev.sparkContext.getConf)

    val rpcToLocalAddress = connector.withCassandraClientDo { client =>
      val ring = client.describe_local_ring(keyspace)
      for {
        tr <- ring
        rpcIps = tr.rpc_endpoints.map(InetAddress.getByName)
        localIps = tr.endpoints.map(InetAddress.getByName)
        (rpc, local) <- (rpcIps).zip(localIps)
      } yield (rpc, local)
    }.toMap
    val localToRpcAddress = rpcToLocalAddress.map { case (k, v) => (v -> k) }

    split match {
      case epp: ReplicaPartition =>
        epp.endpoints.flatMap { origInet =>
          val rpcIps = localToRpcAddress.get(origInet)
          val localIps = rpcToLocalAddress.get(origInet)
          val possibleIps = Seq(Some(origInet), rpcIps, localIps).flatMap(ip => ip)
          possibleIps.flatMap(ip => Seq(ip.getHostAddress, ip.getHostName))
        }.toSeq.distinct
      case other: Partition => throw new IllegalArgumentException(
        "CassandraPartitionedRDD doesn't have Endpointed Partitions. This should be impossible.")
    }
  }
}
