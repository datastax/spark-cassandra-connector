package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.rdd.CqlWhereClause
import org.apache.spark.Partition

/** Stores a CQL `WHERE` predicate matching a range of tokens. */
case class CqlTokenRange(cql: String)

/** Metadata describing Cassandra table partition processed by a single Spark task.
  * Beware the term "partition" is overloaded. Here, in the context of Spark,
  * it means an arbitrary collection of rows that can be processed locally on a single Cassandra cluster node.
  * A `CassandraPartition` typically contains multiple CQL partitions, i.e. rows identified by different values of
  * the CQL partitioning key.
  */
trait CassandraPartition extends Partition {
  def endpoints: Iterable[InetAddress]
}

/** Metadata describing Cassandra table partition processed by a single Spark task.
  * A `CassandraCustomPartition` typically contains rows identified to be on one Cassandra node
  *
  * @param index identifier of the partition, used internally by Spark
  * @param endpoints which nodes the data partition is located on
  * @param tokenRanges token ranges determining the row set to be fetched
  * @param rowCount estimated total row count in a partition
  */
case class CassandraRingPartition(index : Int,
                                  endpoints : Iterable[InetAddress],
                                  tokenRanges: Iterable[CqlTokenRange],
                                  rowCount: Long) extends CassandraPartition

/** Metadata describing Cassandra table partition processed by a single Spark task.
  * A `CassandraCustomPartition` typically contains one row identified by combination of partitioning keys
  * from the original cql request.
  *
  * @param index identifier of the partition, used internally by Spark
  * @param endpoints which nodes the data partition is located on
  * @param where  'where' part that identify data in this request
  */
case class CassandraCustomPartition(index: Int,
                              endpoints: Iterable[InetAddress],
                              where: CqlWhereClause) extends CassandraPartition

