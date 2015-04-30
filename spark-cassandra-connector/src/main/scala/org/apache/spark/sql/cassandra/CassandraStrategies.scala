package org.apache.spark.sql.cassandra

import org.apache.spark.sql.{Strategy, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, SparkPlan}
import org.apache.spark.sql.sources.{CreateTableUsingAsSelect, CreateTableUsing}

private[cassandra] trait CassandraStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SQLContext#SparkPlanner =>

  object CassandraDDLStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTableUsing(
      tableName, userSpecifiedSchema, provider, false, opts, allowExisting, managedIfNoPath) =>
        ExecutedCommand(
          CreateMetastoreDataSource(
            tableName, userSpecifiedSchema, provider, opts, allowExisting)) :: Nil
      case CreateTableUsingAsSelect(tableName, provider, false, mode, opts, query) =>
        val cmd =
          CreateMetastoreDataSourceAsSelect(tableName, provider, mode, opts, query)
        ExecutedCommand(cmd) :: Nil
      case _ => Nil
    }
  }
}