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
    extensions.injectResolutionRule( session => CassandraMetaDataRule)
    try {
      val injectFunction =
        extensions.getClass.getMethod(
          "injectFunction",classOf[Tuple2[
          catalyst.FunctionIdentifier,
          Seq[Expression] => Expression]])

      injectFunction.invoke(
        extensions,
        (FunctionIdentifier("writetime"),
        (input: Seq[Expression]) => CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder(input)))

      injectFunction.invoke(
        extensions,
        (FunctionIdentifier("ttl"),
        (input: Seq[Expression]) => CassandraMetadataFunction.cassandraTTLFunctionBuilder(input)))
    } catch {
      case e @ (_:NoSuchMethodException | _:NoSuchMethodError) => logWarning(
        """Unable to register Cassandra Specific functions CassandraTTL and CassandraWriteTime
          |because of Spark version, use functionRegistry.registerFunction to add
          |the functions.""".stripMargin)
    }
  }
}
