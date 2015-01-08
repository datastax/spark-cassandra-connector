package org.apache.spark.sql.cassandra

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.columnar.InMemoryRelation
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

    private class PredicatePushDown(predicates: Seq[Expression], relation: CassandraRelation) {

      private def isEqualTo(predicate: Expression): Boolean =
        predicate.isInstanceOf[EqualTo]

      private def isIn(predicate: Expression) : Boolean =
        predicate.isInstanceOf[In]

      private def isRangeComparison(predicate: Expression) : Boolean = predicate match {
        case _: LessThan           => true
        case _: LessThanOrEqual    => true
        case _: GreaterThan        => true
        case _: GreaterThanOrEqual => true
        case _                     => false
      }

      private def isSingleColumn(predicate: Expression) =
        predicate.references.size == 1

      private val partitionKeyColumns = relation.partitionColumns.map(_.name)
      private val clusteringColumns = relation.clusterColumns.map(_.name)
      private val indexedColumns = relation.indexedColumns.map(_.name)
      private val regularColumns = relation.regularColumns.map(_.name)
      private val allColumns = partitionKeyColumns ++ clusteringColumns ++ regularColumns

      private val singleColumnPredicates = predicates.filter(isSingleColumn)

      private val eqPredicates = singleColumnPredicates.filter(isEqualTo)
      private val eqPredicatesByName = eqPredicates.groupBy(predicateColumnName)
        .mapValues(_.take(1))       // take(1) in order not to push down more than one EQ predicate for the same column
        .withDefaultValue(Seq.empty)

      private val inPredicates = singleColumnPredicates.filter(isIn)
      private val inPredicatesByName = inPredicates.groupBy(predicateColumnName)
        .mapValues(_.take(1))      // take(1) in order not to push down more than one IN predicate for the same column
        .withDefaultValue(Seq.empty)

      private val rangePredicates = singleColumnPredicates.filter(isRangeComparison)
      private val rangePredicatesByName = rangePredicates.groupBy(predicateColumnName).withDefaultValue(Seq.empty)

      /** Returns the only column name referenced in the predicate */
      private def predicateColumnName(predicate: Expression) = {
        require(predicate.references.size == 1, "Given predicate is not a single column predicate: " + predicate)
        predicate.references.head.name
      }

      /** Returns a first non-empty sequence. If not found, returns an empty sequence. */
      private def firstNonEmptySeq[T](sequences: Seq[T]*): Seq[T] =
        sequences.find(_.nonEmpty).getOrElse(Seq.empty[T])

      /**
       * Selects partition key predicates for pushdown:
       * 1. Partition key predicates must be equality or IN predicates.
       * 2. Only the last partition key column predicate can be an IN.
       * 3. All partition key predicates must be used or none.
       */
      private val partitionKeyPredicatesToPushDown: Seq[Expression] = {
        val (eqColumns, otherColumns) = partitionKeyColumns.span(eqPredicatesByName.contains)
        val inColumns = otherColumns.headOption.toSeq.filter(inPredicatesByName.contains)
        if (eqColumns.size + inColumns.size == partitionKeyColumns.size)
          eqColumns.flatMap(eqPredicatesByName) ++ inColumns.flatMap(inPredicatesByName)
        else
          Nil
      }

      /**
       * Selects clustering key predicates for pushdown:
       * 1. Clustering column predicates must be equality predicates, except the last one.
       * 2. The last predicate is allowed to be an equality or a range predicate.
       * 3. The last predicate is allowed to be an IN predicate only if it was preceded by an equality predicate.
       * 4. Consecutive clustering columns must be used, but, contrary to partition key, the tail can be skipped.
       */
      private val clusteringColumnPredicatesToPushDown: Seq[Expression] = {
        val (eqColumns, otherColumns) = clusteringColumns.span(eqPredicatesByName.contains)
        val eqPredicates = eqColumns.flatMap(eqPredicatesByName)
        val optionalNonEqPredicate = for {
          c <- otherColumns.headOption.toSeq
          p <- firstNonEmptySeq(rangePredicatesByName(c), inPredicatesByName(c).filter(_ => eqColumns.nonEmpty))
        } yield p

        eqPredicates ++ optionalNonEqPredicate
      }

      /**
       * Selects indexed and regular column predicates for pushdown:
       * 1. At least one indexed column must be present in an equality predicate to be pushed down.
       * 2. Regular column predicates can be either equality or range predicates.
       * 3. If multiple predicates use the same column, equality predicates are preferred over range predicates.
       */
      private val indexedColumnPredicatesToPushDown: Seq[Expression] = {
        val inPredicateInPrimaryKey = partitionKeyPredicatesToPushDown.exists(isIn)
        val eqIndexedColumns = indexedColumns.filter(eqPredicatesByName.contains)
        val eqIndexedPredicates = eqIndexedColumns.flatMap(eqPredicatesByName)
        val nonIndexedPredicates = for {
          c <- allColumns if !eqIndexedColumns.contains(c)
          p <- firstNonEmptySeq(eqPredicatesByName(c), rangePredicatesByName(c))
        } yield p

        if (!inPredicateInPrimaryKey && eqIndexedColumns.nonEmpty)
          eqIndexedPredicates ++ nonIndexedPredicates
        else
          Nil
      }

      val predicatesToPushDown = (
        partitionKeyPredicatesToPushDown ++
        clusteringColumnPredicatesToPushDown ++
        indexedColumnPredicatesToPushDown).distinct

      val predicatesToPreserve = predicates.filterNot(predicatesToPushDown.toSet.contains)
    }


    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: CassandraRelation) =>
        logInfo(s"projectList: ${projectList.toString()}")
        logInfo(s"predicates: ${predicates.toString()}")
        val pushDown = new PredicatePushDown(predicates, relation)
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
