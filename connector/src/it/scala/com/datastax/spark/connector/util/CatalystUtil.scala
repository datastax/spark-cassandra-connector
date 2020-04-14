package com.datastax.spark.connector.util

import com.datastax.spark.connector.datasource.CassandraScan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

object CatalystUtil {

  def findCassandraScan(sparkPlan: SparkPlan): Option[CassandraScan] = {
    sparkPlan.collectFirst{ case BatchScanExec(_, scan: CassandraScan) => scan}
  }
}
