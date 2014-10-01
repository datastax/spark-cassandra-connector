package org.apache.spark.sql.cassandra

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer

private[cassandra] trait CassandraStrategies {

  // Possibly being too clever with types here... or not clever enough.
  self: SQLContext#SparkPlanner =>

  val cassandraContext: CassandraSQLContext

  private[this] def pushdownAble(predicate: Expression) : Boolean = predicate match {
    case e: EqualTo             => true
    case l: LessThan            => true
    case le: LessThanOrEqual    => true
    case g: GreaterThan         => true
    case ge: GreaterThanOrEqual => true
    case _                      => false
  }

  private[this] def isIN(predicate: Expression) : Boolean = predicate match {
    case l: In => true
    case _     => false
  }

  private[this] def isEqualTo(predicate: Expression) : Boolean = predicate match {
    case e: EqualTo => true
    case _          => false
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(table: CassandraRelation, partition, child, overwrite) =>
        InsertIntoCassandraTable(table, planLater(child), overwrite)(cassandraContext) :: Nil
      case logical.InsertIntoTable(
      InMemoryRelation(_, _, _, CassandraTableScan(_, table, _)), partition, child, overwrite) =>
        InsertIntoCassandraTable(table, planLater(child), overwrite)(cassandraContext) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a CassandraTableScan.  Partition pruning predicates are also detected and
   * applied.
   *  1. Only push down none-partition column predicates with =, >, <, >=, <= predicate
   *  2. Only push down primary key column predicates with = or IN predicate.
   *  3. If there are regular columns in the pushdown predicates. There should have
   *     at least one EQ expression on an indexed column and no IN predicates.
   *  4. All partition column predicates must in the predicates to be pushed down,
   *     only the last part of the partition key can be IN predicate. For each partition column,
   *     only one predicate is allowed.
   *  5. For cluster column predicates, only last predicate can be none-EQ predicate
   *     including IN predicate, and preceding column predicates must be EQ predicates.
   *     If there is only one cluster column predicates, the predicates could be any predicate.
   *  6. There is no pushdown predicates if there is any OR condition or NOT IN condition
   */
  //TODO: index on collection pushdown
  object CassandraTableScans extends Strategy with Logging {

    def partitionColumnPushDown(partitionColumnPredicates: Seq[Expression], partitionColumns: Seq[String]):
      (Boolean, Boolean, Seq[Expression], Seq[Expression]) = {
        val lastPartitionColumn = partitionColumns.last
        var partitionColumnPushdown = ListBuffer[Expression]()
        var partitionColumnOther = ListBuffer[Expression]()
        var namePredicateMap = Map[String, Expression]()
        for (predicate <- partitionColumnPredicates) {
          val name = predicate.references.map(_.name).head
          if (!namePredicateMap.contains(name) && 
              (isEqualTo(predicate) || name == lastPartitionColumn && isIN(predicate))) {
            partitionColumnPushdown += predicate
            namePredicateMap += name -> predicate
          } else {
            partitionColumnOther += predicate
          }
        }

        val partitionColumnPd = partitionColumnPushdown.toSeq
        val partitionColumnRemaining = partitionColumnOther.toSeq
        if (partitionColumnPd.size == partitionColumns.size) {
          if (partitionColumnPd.filter(isIN).nonEmpty) {
            (true, true, partitionColumnPd, partitionColumnRemaining)
          } else {
            (true, false, partitionColumnPd, partitionColumnRemaining)
          }
        } else {
          (false, false, partitionColumnPd, partitionColumnRemaining)
        }
    }

    def clusterColumnPushDown(clusterColumnPredicates: Seq[Expression], clusterColumns: Seq[String]):
      (Boolean, Seq[Expression], Seq[Expression]) = {
        var clusterColumnPushdown = ListBuffer[Expression]()
        var clusterColumnOther = ListBuffer[Expression]()
        var namePredicateMap = Map[String, Expression]()
        var clusterColumnHasINClause = false
        for (predicate <- clusterColumnPredicates) {
          val name = predicate.references.map(_.name).head
          if (!namePredicateMap.contains(name) && isEqualTo(predicate)) {
            clusterColumnPushdown += predicate
            namePredicateMap += name -> predicate
          } else {
              clusterColumnOther += predicate
          }
          if (!clusterColumnHasINClause && isIN(predicate))
            clusterColumnHasINClause = true
        }

        var restoreFollowingPredicate = false
        var restoreFollowingColumns = ListBuffer[String]()
        var lastColumnPushdown = new String("")
        for (column <- clusterColumns) {
          if (restoreFollowingPredicate)
            restoreFollowingColumns += column
          if (!restoreFollowingPredicate && !namePredicateMap.contains(column)) {
            restoreFollowingColumns += column
            lastColumnPushdown = column
            restoreFollowingPredicate = true
          }
        }

        logInfo(s"Cluster column intermediate pushed down predicates: ${clusterColumnPushdown.toString}")

        val (other, pd) = clusterColumnPushdown.toSeq.partition {
          predicate => predicate.references.map(_.name).toSet.subsetOf(restoreFollowingColumns.toSet)
        }

        logInfo(s"Last cluster column pushed down: $lastColumnPushdown")
        logInfo(s"Other cluster column predicates: ${clusterColumnOther.toString}")

        val (push, left) = clusterColumnOther.toSeq.partition {
          predicate => predicate.references.map(_.name).head == lastColumnPushdown && (isIN(predicate) || pushdownAble(predicate))
        }

        (clusterColumnHasINClause, pd ++ push, other ++ left)
    }

    // Final predicate pushdown
    def pushdown(partitionColumnPushdownable: Boolean, partitionColumnHasINClause: Boolean,
        partitionColumnPd: Seq[Expression], partitionColumnRemaining: Seq[Expression], partitionColumnPredicates: Seq[Expression],
        clusterColumnHasINClause: Boolean, clusterColumnPd: Seq[Expression], clusterColumnRemaining: Seq[Expression],
        indexedColumnPd: Seq[Expression], regularColumnPd: Seq[Expression], regularColumnOther:Seq[Expression],
        regularColumnPredicates: Seq[Expression]) : (Seq[Expression], Seq[Expression]) = {

        // No regular column predicates pushdown if there are no indexed column predicates or no EQ indexed column
        // predicates
        if (indexedColumnPd.isEmpty || indexedColumnPd.filter(isEqualTo).isEmpty) {
          // Only partition column predicates and cluster column predicates are pushed down
          if (partitionColumnPushdownable) {
            logInfo("Partition column predicates are pushed down.")
            (clusterColumnPd ++ partitionColumnPd, regularColumnPredicates ++ clusterColumnRemaining ++ partitionColumnRemaining)
          } else {
            (clusterColumnPd, regularColumnPredicates ++ partitionColumnPredicates)
          }
        } else {
          // No IN predicate in primary key column predicates
          val (pushdown, remain) =
            if (partitionColumnPushdownable && !partitionColumnHasINClause)
              (regularColumnPd ++ partitionColumnPd, regularColumnOther ++ partitionColumnRemaining)
            else
              (regularColumnPd, regularColumnOther ++ partitionColumnPredicates)

          if (!clusterColumnHasINClause) {
            (pushdown ++ clusterColumnPd, remain ++ clusterColumnRemaining)
          } else {
            val (other, pd) = clusterColumnPd.partition(isIN)
            (pushdown ++ pd, remain ++ clusterColumnRemaining ++ other)
          }
        }
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: CassandraRelation) =>

        logInfo(s"projectList: ${projectList.toString()}")
        logInfo(s"predicates: ${predicates.toString()}")

        val indexedColumns = relation.indexedColumns.map(_.name)
        val regularColumns = relation.regularColumns.map(_.name)
        val partitionColumns = relation.partitionColumns.map(_.name)
        val clusterColumns = relation.clusterColumns.map(_.name)
        val allColumns = partitionColumns ++ clusterColumns ++ regularColumns

        val allColumnPredicates = predicates.filter(_.references.map(_.name).toSet.subsetOf(allColumns.toSet))
        if (allColumnPredicates.isEmpty) {
          pruneFilterProject(
            projectList,
            predicates,
            identity[Seq[Expression]],
            CassandraTableScan(_, relation, Seq.empty[Expression])(cassandraContext)) :: Nil
        } else {
          // Partition column predicates pushdown
          val partitionColumnPredicates = predicates.filter(_.references.map(_.name).toSet.subsetOf(partitionColumns.toSet))
          val (partitionColumnPushdownable, partitionColumnHasINClause, partitionColumnPd, partitionColumnRemaining)
            = partitionColumnPushDown(partitionColumnPredicates, partitionColumns)

          // Cluster column predicates pushdown
          val clusterColumnPredicates = predicates.filter(_.references.map(_.name).toSet.subsetOf(clusterColumns.toSet))
          val (clusterColumnHasINClause, clusterColumnPd, clusterColumnRemaining) = clusterColumnPushDown(clusterColumnPredicates, clusterColumns)

          logInfo(s"Cluster column final pushdown predicates: ${clusterColumnPd.toString}")
          logInfo(s"Cluster column remaining predicates: ${clusterColumnRemaining.toString}")

          val regularColumnPredicates = predicates.filter(_.references.map(_.name).toSet.subsetOf(regularColumns.toSet))
          val (regularColumnPd, regularColumnOther) = regularColumnPredicates.partition { pushdownAble }
          val indexedColumnPd = regularColumnPd.filter(_.references.map(_.name).toSet.subsetOf(indexedColumns.toSet))

          logInfo(s"Indexed column pushdownable predicates: ${indexedColumnPd.toString()}")

          val (pushdownPredicates, otherPredicates) = pushdown(partitionColumnPushdownable, partitionColumnHasINClause,
            partitionColumnPd, partitionColumnRemaining, partitionColumnPredicates,
            clusterColumnHasINClause, clusterColumnPd, clusterColumnRemaining,
            indexedColumnPd, regularColumnPd, regularColumnOther, regularColumnPredicates)

          logInfo(s"pushdown predicates:  ${pushdownPredicates.toString()}")
          logInfo(s"remaining predicates: ${otherPredicates.toString()}")

          pruneFilterProject(
            projectList,
            otherPredicates,
            identity[Seq[Expression]],
            CassandraTableScan(_, relation, pushdownPredicates)(cassandraContext)) :: Nil
        }
      case _ =>
        Nil
    }
  }
}
