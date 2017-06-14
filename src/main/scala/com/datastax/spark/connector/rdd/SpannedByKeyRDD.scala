package com.datastax.spark.connector.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import com.datastax.spark.connector.util.SpanningIterator

/**
 * Similar to [[SpannedRDD]] but, instead of extracting the key by the given function,
 * it groups binary tuples by the first element of each tuple.
 */
private[connector] class SpannedByKeyRDD[K, V](parent: RDD[(K, V)]) extends RDD[(K, Seq[V])](parent) {

  override protected def getPartitions = parent.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) = {
    val parentIterator = parent.iterator(split, context)
    def keyFunction(item: (K, V)) = item._1
    def extractValues(group: (K, Seq[(K, V)])) = (group._1, group._2.map(_._2))
    new SpanningIterator(parentIterator, keyFunction).map(extractValues)
  }

}
