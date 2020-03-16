package com.datastax.spark.connector

import com.datastax.spark.connector.rdd.SpannedByKeyRDD
import org.apache.spark.rdd.RDD

class PairRDDFunctions[K, V](rdd: RDD[(K, V)]) extends Serializable {

  /**
   * Groups items with the same key, assuming the items with the same key are next to each other
   * in the collection. It does not perform shuffle, therefore it is much faster than using
   * much more universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables,
   * the key must represent a prefix of the primary key, containing at least the partition key of the
   * Cassandra table. */
  def spanByKey: RDD[(K, Seq[V])] =
    new SpannedByKeyRDD[K, V](rdd)

}
