package com.datastax.spark.connector.japi

import java.lang.{Iterable => JIterable}

import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import com.datastax.spark.connector._

class PairRDDJavaFunctions[K : ClassTag, V](rdd: RDD[(K, V)]) extends RDDJavaFunctions(rdd) {

  /** Groups items with the same key, assuming the items with the same key are next to each other
    * in the collection. It does not perform shuffle, therefore it is much faster than using
    * much more universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables,
    * the key must represent a prefix of the primary key, containing at least the partition key of the
    * Cassandra table. */
  def spanByKey(): JavaPairRDD[K, JIterable[V]] = {
    new JavaPairRDD(rdd.spanByKey.mapValues(asJavaIterable))
  }
}
