package com.datastax.spark.connector

import org.apache.spark.sql.{SparkSessionExtensions, catalyst}
import org.apache.spark.sql.cassandra.execution.DSEDirectJoinStrategy
import org.apache.spark.sql.cassandra.{CassandraMetaDataRule, CassandraMetadataFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.catalyst.expressions.Expression

class DseSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(DSEDirectJoinStrategy.apply)
    extensions.injectResolutionRule( session => CassandraMetaDataRule)
    try {
      val injectFunction =
        extensions.getClass.getMethod(
          "injectFunction",
          classOf[catalyst.FunctionIdentifier],
          classOf[Seq[Expression] => Expression])

      injectFunction.invoke(
        extensions,
        FunctionIdentifier("writetime"),
        (input: Seq[Expression]) => CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder(input))

      injectFunction.invoke(
        extensions,
        FunctionIdentifier("ttl"),
        (input: Seq[Expression]) => CassandraMetadataFunction.cassandraTTLFunctionBuilder(input))
    } catch {
      case _:NoSuchMethodException =>
      case _:NoSuchMethodError => logWarning(
        """Unable to register DSE Specific functions CassandraTTL and CassandraWriteTime
          |because of spark version, use functionRegistry.registerFunction to add
          |the functions.""".stripMargin)
    }
  }
}
