package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.apache.spark.Partition
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}
import org.apache.spark.sql.connector.read.InputPartition

/** Stores a CQL `WHERE` predicate matching a range of tokens. */
case class CqlTokenRange[V, T <: Token[V]](range: TokenRange[V, T])(implicit tf: TokenFactory[V, T]) {

  require(!range.isWrappedAround)

  def cql(pk: String): (String, Seq[Any]) =
    if (range.start == tf.minToken && range.end == tf.minToken)
      (s"token($pk) >= ?", Seq(range.start.value))
    else if (range.start == tf.minToken)
      (s"token($pk) <= ?", Seq(range.end.value))
    else if (range.end == tf.minToken)
      (s"token($pk) > ?", Seq(range.start.value))
    else
      (s"token($pk) > ? AND token($pk) <= ?", Seq(range.start.value, range.end.value))
}

trait EndpointPartition extends Partition {
  def endpoints: Array[String]
}

/** Metadata describing Cassandra table partition processed by a single Spark task.
  * Beware the term "partition" is overloaded. Here, in the context of Spark,
  * it means an arbitrary collection of rows that can be processed locally on a single Cassandra cluster node.
  * A `CassandraPartition` typically contains multiple CQL partitions, i.e. rows identified by different values of
  * the CQL partitioning key.
  *
  * @param index identifier of the partition, used internally by Spark
  * @param endpoints which nodes the data partition is located on
  * @param tokenRanges token ranges determining the row set to be fetched
  * @param dataSize estimated amount of data in the partition
  */
case class CassandraPartition[V, T <: Token[V]] (
  index: Int,
  endpoints: Array[String],
  tokenRanges: Iterable[CqlTokenRange[V, T]],
  dataSize: Long) extends EndpointPartition with InputPartition {

  override def preferredLocations(): Array[String] = endpoints
}

