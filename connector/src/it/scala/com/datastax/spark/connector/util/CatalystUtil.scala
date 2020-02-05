package com.datastax.spark.connector.util

import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._

object CatalystUtil {
  @scala.annotation.tailrec
  def findCassandraTableScanRDD(sparkPlan: SparkPlan): Option[CassandraTableScanRDD[_]] = {
    def _findCassandraTableScanRDD(rdd: RDD[_]): Option[CassandraTableScanRDD[_]] = {
      rdd match {
        case ctsrdd: CassandraTableScanRDD[_] => Some(ctsrdd)
        case other: RDD[_] => other.dependencies.iterator
          .flatMap(dep => _findCassandraTableScanRDD(dep.rdd)).take(1).toList.headOption
      }
    }

    sparkPlan match {
      case prdd: RDDScanExec => _findCassandraTableScanRDD(prdd.rdd)
      case prdd: RowDataSourceScanExec => _findCassandraTableScanRDD(prdd.rdd)
      case filter: FilterExec => findCassandraTableScanRDD(filter.child)
      case wsc: WholeStageCodegenExec => findCassandraTableScanRDD(wsc.child)
      case _ => None
    }
  }
}
