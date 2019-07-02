package com.datastax.spark.connector.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import com.datastax.spark.connector.util.SpanningIterator

/**
 * Groups items with the same key, assuming items with the same key are next to each other in
 * the parent collection. Contrary to Spark GroupedRDD, it does not perform shuffle, therefore it
 * is much faster. A key for each item is obtained by calling a given function.
 *
 * This RDD is very useful for grouping data coming out from Cassandra, because they are already
 * coming in order of partitioning key i.e. it is not possible for two rows
 * with the same partition key to be in different Spark partitions.
 *
 * @param parent parent RDD
 * @tparam K type of keys
 * @tparam T type of elements to be grouped together
 */
private[connector] class SpannedRDD[K, T](parent: RDD[T], f: T => K)
  extends RDD[(K, Iterable[T])](parent) {

  override protected def getPartitions = parent.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) =
    new SpanningIterator(parent.iterator(split, context), f)

}

