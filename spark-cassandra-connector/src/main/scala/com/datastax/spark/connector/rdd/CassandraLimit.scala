package com.datastax.spark.connector.rdd

sealed trait CassandraLimit

case class CassandraPartitionLimit(rowsNumber: Long) extends CassandraLimit {
  require(rowsNumber > 0, s"$rowsNumber <= 0. Per Partition Limits must be greater than 0")
}
case class SparkPartitionLimit(rowsNumber: Long) extends CassandraLimit {
  require(rowsNumber > 0, s"$rowsNumber <= 0. Limits must be greater than 0")
}

object CassandraLimit {

  def limitToClause
  (limit: Option[CassandraLimit]): String = limit match {
    case Some(SparkPartitionLimit(rowsNumber)) => s"LIMIT $rowsNumber"
    case Some(CassandraPartitionLimit(rowsNumber)) => s"PER PARTITION LIMIT $rowsNumber"
    case None => ""
  }

  def limitForIterator(limit: Option[CassandraLimit]): Option[Long] = limit.collect {
    case SparkPartitionLimit(rowsNumber) => rowsNumber
  }
}

