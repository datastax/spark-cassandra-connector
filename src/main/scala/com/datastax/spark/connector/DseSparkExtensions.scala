package com.datastax.spark.connector

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.cassandra.execution.DSEDirectJoinStrategy
import org.apache.spark.sql.cassandra.{CassandraMetaDataRule, CassandraMetadataFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier

import com.datastax.spark.connector.util.Logging

class DseSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(DSEDirectJoinStrategy.apply)
    extensions.injectPostHocResolutionRule{ session => CassandraMetaDataRule }
    try {
      extensions.injectFunction(FunctionIdentifier("writetime"), CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder)
      extensions.injectFunction(FunctionIdentifier("ttl"), CassandraMetadataFunction.cassandraTTLFunctionBuilder)
    } catch {
      case _:NoSuchMethodError => logWarning("Unable to register DSE Specific functions CassandraTTL and CassandraWriteTime because of spark version, use functionRegistry.registerFunction to add the functions.")
    }
  }
}
