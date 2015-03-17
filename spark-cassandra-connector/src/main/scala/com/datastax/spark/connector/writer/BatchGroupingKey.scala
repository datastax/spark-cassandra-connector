package com.datastax.spark.connector.writer

sealed trait BatchGroupingKey

object BatchGroupingKey {

  /** Any row can be added to any batch. This works the same as previous batching implementation. */
  case object None extends BatchGroupingKey

  /** Each batch is associated with a set of replicas. If a set of replicas for the inserted row is
    * the same as it is for a batch, the row can be added to the batch. */
  case object ReplicaSet extends BatchGroupingKey

  /** Each batch is associated with a partition key. If the partition key of the inserted row is the
    * same as it is for a batch, the row can be added to the batch. */
  case object Partition extends BatchGroupingKey

  def apply(name: String): BatchGroupingKey = name.toLowerCase match {
    case "none" => None
    case "replica_set" => ReplicaSet
    case "partition" => Partition
    case _ => throw new IllegalArgumentException(s"Invalid batch level: $name")
  }
}
