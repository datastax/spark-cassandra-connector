package com.datastax.spark.connector

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.cassandra.execution.CassandraDirectJoinStrategy

class DseSparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(CassandraDirectJoinStrategy.apply)
  }
}
