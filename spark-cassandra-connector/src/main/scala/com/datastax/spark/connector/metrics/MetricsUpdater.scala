package com.datastax.spark.connector.metrics

import org.apache.spark.SparkEnv

trait MetricsUpdater {
  def finish(): Long

  def forceReport(): Unit = {
    val env = SparkEnv.get
    if (env != null) {
      env.metricsSystem.report()
    }
  }
}
