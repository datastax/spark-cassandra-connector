package org.apache.spark

import org.apache.spark.rdd.UnionPartition

object DseTestUtil {

  //UnionPartition is private[spark], expose it's parentPartition method
  def getParentPartition[T](unionPartition: Partition): Partition = {
    unionPartition match { case unionPartition: UnionPartition[T @unchecked] => unionPartition.parentPartition}
  }
}
