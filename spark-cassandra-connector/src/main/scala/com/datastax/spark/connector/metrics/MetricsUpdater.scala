package com.datastax.spark.connector.metrics

import org.apache.spark.SparkEnv

trait MetricsUpdater {
  def finish(): Long

  def forceReport(): Unit = {
    Option(SparkEnv.get).foreach(_.metricsSystem.report())
  }
}
