package com.datastax.driver.spark.rdd

import java.net.InetAddress

import org.apache.spark.Partition

case class CqlTokenRange(cql: String)

case class CassandraPartition(index: Int,
                              endpoints: Iterable[InetAddress],
                              tokenRanges: Iterable[CqlTokenRange],
                              rowCount: Long) extends Partition

