package com.datastax.spark.connector

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.cassandra.execution.DSEDirectJoinStrategy
import org.apache.spark.sql.cassandra.{CassandraMetaDataRule, CassandraMetadataFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier

class DseSparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(DSEDirectJoinStrategy.apply)
    extensions.injectPostHocResolutionRule{ session => CassandraMetaDataRule }
    extensions.injectFunction(FunctionIdentifier("writetime"), CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder)
    extensions.injectFunction(FunctionIdentifier("ttl"), CassandraMetadataFunction.cassandraTTLFunctionBuilder)
  }
}
