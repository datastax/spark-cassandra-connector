package org.apache.spark.sql.cassandra

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

private[cassandra] trait CassandraStrategies {

  // Possibly being too clever with types here... or not clever enough.
  self: SQLContext#SparkPlanner =>

  val cassandraContext: CassandraSQLContext

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(table: CassandraRelation, partition, child, overwrite) =>
        InsertIntoCassandraTable(table, planLater(child), overwrite)(cassandraContext) :: Nil
      case _ => Nil
    }
  }

  /** Retrieves data using a CassandraTableScan.
    * Partition pruning predicates are also detected an applied. */
  object CassandraTableScans extends Strategy with Logging {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: CassandraRelation) =>
        logInfo(s"projectList: ${projectList.toString()}")
        logInfo(s"predicates: ${predicates.toString()}")
        val pushDown = new PredicatePushDown(predicates, relation.tableDef)
        val pushdownPredicates = pushDown.predicatesToPushDown
        val otherPredicates = pushDown.predicatesToPreserve
        logInfo(s"pushdown predicates:  ${pushdownPredicates.toString()}")
        logInfo(s"remaining predicates: ${otherPredicates.toString()}")

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          CassandraTableScan(_, relation, pushdownPredicates)(cassandraContext)) :: Nil
      case _ =>
        Nil
    }
  }
}
