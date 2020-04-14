package com.datastax.spark.connector

import org.apache.spark.sql.{SparkSessionExtensions, catalyst}
import org.apache.spark.sql.cassandra.execution.CassandraDirectJoinStrategy
import org.apache.spark.sql.cassandra.{CassandraMetaDataRule, CassandraMetadataFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.catalyst.expressions.Expression

class CassandraSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(CassandraDirectJoinStrategy.apply)
    extensions.injectResolutionRule(session => CassandraMetaDataRule)
    extensions.injectFunction(CassandraMetadataFunction.cassandraTTLFunctionDescriptor)
    extensions.injectFunction(CassandraMetadataFunction.cassandraWriteTimeFunctionDescriptor)
  }
}
