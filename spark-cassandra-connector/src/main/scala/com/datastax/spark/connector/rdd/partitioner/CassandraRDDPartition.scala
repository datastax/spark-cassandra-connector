package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.apache.spark.Partition

/** Stores a CQL `WHERE` predicate matching a range of tokens. */
case class CqlTokenRange(cql: String, values: Any*)

trait EndpointPartition extends Partition {
  def endpoints: Iterable[InetAddress]
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
  * @param rowCount estimated total row count in a partition
  */
case class CassandraPartition(index: Int,
                              endpoints: Iterable[InetAddress],
                              tokenRanges: Iterable[CqlTokenRange],
                              rowCount: Long) extends EndpointPartition

