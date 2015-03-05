package com.datastax.spark.connector.rdd

import org.apache.spark.SparkConf

import com.datastax.driver.core.ConsistencyLevel

/** Read settings for RDD
  *
  * @param splitSize number of Cassandra partitions to be read in a single Spark task
  * @param fetchSize number of CQL rows to fetch in a single round-trip to Cassandra
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality */
case class ReadConf(
  splitSize: Int = ReadConf.DefaultSplitSize,
  fetchSize: Int = ReadConf.DefaultFetchSize,
  consistencyLevel: ConsistencyLevel = ReadConf.DefaultConsistencyLevel)


object ReadConf {
  val DefaultSplitSize = 100000
  val DefaultFetchSize = 1000
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE

  def fromSparkConf(conf: SparkConf, cluster: Option[String] = None): ReadConf = {
    cluster match {
      case Some(c) =>
        val clusterName = cluster.get
        ReadConf(
          fetchSize = conf.getInt("spark." + clusterName + ".cassandra.input.page.row.size", DefaultFetchSize),
          splitSize = conf.getInt("spark." + clusterName + ".cassandra.input.split.size", DefaultSplitSize),
          consistencyLevel = ConsistencyLevel.valueOf(
            conf.get("spark." + clusterName + ".cassandra.input.consistency.level", DefaultConsistencyLevel.name()))
        )
      case None =>
        ReadConf(
          fetchSize = conf.getInt("spark.cassandra.input.page.row.size", DefaultFetchSize),
          splitSize = conf.getInt("spark.cassandra.input.split.size", DefaultSplitSize),
          consistencyLevel = ConsistencyLevel.valueOf(
            conf.get("spark.cassandra.input.consistency.level", DefaultConsistencyLevel.name()))
        )
    }

  }
}

