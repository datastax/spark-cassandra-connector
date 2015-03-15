package com.datastax.spark.connector.writer

sealed trait BatchLevel

object BatchLevel {

  /** Any row can be added to any batch. This works the same as previous batching implementation. */
  case object Any extends BatchLevel

  /** Each batch is associated with a set of replicas. If a set of replicas for the inserted row is
    * the same as it is for a batch, the row can be added to the batch. */
  case object ReplicaSet extends BatchLevel

  /** Each batch is associated with a partition key. If the partition key of the inserted row is the
    * same as it is for a batch, the row can be added to the batch. */
  case object Partition extends BatchLevel

  def apply(name: String): BatchLevel = name.toLowerCase match {
    case "any" => Any
    case "replica_set" => ReplicaSet
    case "partition" => Partition
    case _ => throw new IllegalArgumentException(s"Invalid batch level: $name")
  }
}
