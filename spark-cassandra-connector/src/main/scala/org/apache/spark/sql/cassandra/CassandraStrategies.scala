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

  /**
   * Retrieves data using a CassandraTableScan.
   * Partition pruning predicates are also detected an applied.
   *  1. Only push down no-partition key column predicates with =, >, <, >=, <= predicate
   *  2. Only push down primary key column predicates with = or IN predicate.
   *  3. If there are regular columns in the pushdown predicates, they should have
   *     at least one EQ expression on an indexed column and no IN predicates.
   *  4. All partition column predicates must be included in the predicates to be pushed down,
   *     only the last part of the partition key can be an IN predicate. For each partition column,
   *     only one predicate is allowed.
   *  5. For cluster column predicates, only last predicate can be non-EQ predicate
   *     including IN predicate, and preceding column predicates must be EQ predicates.
   *     If there is only one cluster column predicate, the predicates could be any non-IN predicate.
   *  6. There is no pushdown predicates if there is any OR condition or NOT IN condition.
   *  7. We're not allowed to push down multiple predicates for the same column if any of them
   *     is equality or IN predicate.
   */
  // TODO: index on collection pushdown
  object CassandraTableScans extends Strategy with Logging {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: CassandraRelation) =>

        logInfo(s"projectList: ${projectList.toString()}")
        logInfo(s"predicates: ${predicates.toString()}")

        val pushDown = new PredicatePushDown(predicates, relation.tableDef)
        val pushdownPredicates = pushDown.toPushDown
        val otherPredicates = pushDown.toPreserve

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
