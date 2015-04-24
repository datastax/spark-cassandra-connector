package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}
import org.apache.cassandra.thrift.TokenRange

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
        "partitioned by ReplicaPartitioner. Unable to do any work with data locality.")
    }
  }

  /**
   * This method currently uses thrift to determine the local endpoint and rpc endpoints
   * from whatever endpoint the partition belongs to. This gives us a better chance of matching
   * the bound interface of the Spark Executor. In addition we will add the HostName and
   * HostAddress that we get for each of these endpoints to cover all of the logical choices.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case epp: ReplicaPartition =>
      val rpcToLocalAddress = rpcToLocalAddressMap
      val localToRpcAddress = rpcToLocalAddress.map(_.swap)

      epp.endpoints.flatMap { origInet =>
        val rpcIps = localToRpcAddress.get(origInet)
        val localIps = rpcToLocalAddress.get(origInet)
        val possibleIps = Seq(Some(origInet), rpcIps, localIps).flatten
        possibleIps.flatMap(ip => Seq(ip.getHostAddress, ip.getHostName))
      }.toSeq.distinct

    case other: Partition => throw new IllegalArgumentException(
      "CassandraPartitionedRDD doesn't have Endpointed Partitions. PrefferedLocations cannot be" +
        "deterimined")
  }

  private def rpcToLocalAddressMap: Map[InetAddress, InetAddress] = (
    for {
      tr <- localDcRing(keyspace)
      rpcIps = tr.rpc_endpoints.map(InetAddress.getByName)
      localIps = tr.endpoints.map(InetAddress.getByName)
      (rpc, local) <- rpcIps.zip(localIps)
    } yield (rpc, local)).toMap

  private def localDcRing(keyspace: String): Seq[TokenRange] =
    CassandraConnector(prev.sparkContext.getConf)
      .withCassandraClientDo(_.describe_local_ring(keyspace))
}
