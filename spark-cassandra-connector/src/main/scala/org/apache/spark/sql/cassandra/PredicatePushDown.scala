package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.TableDef

/**
 *  Determines which filter predicates can be pushed down to Cassandra.
 *
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
 *
 *  The list of predicates to be pushed down is available in `predicatesToPushDown` property.
 *  The list of predicates that cannot be pushed down is available in `predicatesToPreserve` property.
 *
 * @param predicates list of filter predicates available in the user query
 * @param table Cassandra table definition
 */
class PredicatePushDown[Predicate : PredicateOps](predicates: Set[Predicate], table: TableDef) {

  private val Predicates = implicitly[PredicateOps[Predicate]]

  private val partitionKeyColumns = table.partitionKey.map(_.columnName)
  private val clusteringColumns = table.clusteringColumns.map(_.columnName)
  private val regularColumns = table.regularColumns.map(_.columnName)
  private val allColumns = partitionKeyColumns ++ clusteringColumns ++ regularColumns
  private val indexedColumns = table.columns.filter(_.isIndexedColumn).map(_.columnName)

  private val singleColumnPredicates = predicates.filter(Predicates.isSingleColumnPredicate)

  private val eqPredicates = singleColumnPredicates.filter(Predicates.isEqualToPredicate)
  private val eqPredicatesByName =
    eqPredicates
      .groupBy(Predicates.columnName)
      .mapValues(_.take(1))       // don't push down more than one EQ predicate for the same column
      .withDefaultValue(Set.empty)

  private val inPredicates = singleColumnPredicates.filter(Predicates.isInPredicate)
  private val inPredicatesByName =
    inPredicates
      .groupBy(Predicates.columnName)
      .mapValues(_.take(1))      // don't push down more than one IN predicate for the same column
      .withDefaultValue(Set.empty)

  private val rangePredicates = singleColumnPredicates.filter(Predicates.isRangePredicate)
  private val rangePredicatesByName =
    rangePredicates
      .groupBy(Predicates.columnName)
      .withDefaultValue(Set.empty)

  /** Returns a first non-empty set. If not found, returns an empty set. */
  private def firstNonEmptySet[T](sets: Set[T]*): Set[T] =
    sets.find(_.nonEmpty).getOrElse(Set.empty[T])

  /**
   * Selects partition key predicates for pushdown:
   * 1. Partition key predicates must be equality or IN predicates.
   * 2. Only the last partition key column predicate can be an IN.
   * 3. All partition key predicates must be used or none.
   */
  private val partitionKeyPredicatesToPushDown: Set[Predicate] = {
    val (eqColumns, otherColumns) = partitionKeyColumns.span(eqPredicatesByName.contains)
    val inColumns = otherColumns.headOption.toSeq.filter(inPredicatesByName.contains)
    if (eqColumns.size + inColumns.size == partitionKeyColumns.size)
      (eqColumns.flatMap(eqPredicatesByName) ++ inColumns.flatMap(inPredicatesByName)).toSet
    else
      Set.empty
  }

  /**
   * Selects clustering key predicates for pushdown:
   * 1. Clustering column predicates must be equality predicates, except the last one.
   * 2. The last predicate is allowed to be an equality or a range predicate.
   * 3. The last predicate is allowed to be an IN predicate only if it was preceded by
   *    an equality predicate.
   * 4. Consecutive clustering columns must be used, but, contrary to partition key,
   *    the tail can be skipped.
   */
  private val clusteringColumnPredicatesToPushDown: Set[Predicate] = {
    val (eqColumns, otherColumns) = clusteringColumns.span(eqPredicatesByName.contains)
    val eqPredicates = eqColumns.flatMap(eqPredicatesByName).toSet
    val optionalNonEqPredicate = for {
      c <- otherColumns.headOption.toSeq
      p <- firstNonEmptySet(
        rangePredicatesByName(c),
        inPredicatesByName(c).filter(_ => c == clusteringColumns.last))
    } yield p

    eqPredicates ++ optionalNonEqPredicate
  }

  /**
   * Selects indexed and regular column predicates for pushdown:
   * 1. At least one indexed column must be present in an equality predicate to be pushed down.
   * 2. Regular column predicates can be either equality or range predicates.
   * 3. If multiple predicates use the same column, equality predicates are preferred over range
   *    predicates.
   */
  private val indexedColumnPredicatesToPushDown: Set[Predicate] = {
    val inPredicateInPrimaryKey = partitionKeyPredicatesToPushDown.exists(Predicates.isInPredicate)
    val eqIndexedColumns = indexedColumns.filter(eqPredicatesByName.contains)
    val eqIndexedPredicates = eqIndexedColumns.flatMap(eqPredicatesByName)

    // Don't include partition predicates in nonIndexedPredicates if partition predicates can't
    // be pushed down because we use token range query which already has partition columns in the
    // where clause and it can't have other partial partition columns in where clause any more.
    val nonIndexedPredicates = for {
      c <- allColumns if
        partitionKeyPredicatesToPushDown.nonEmpty && !eqIndexedColumns.contains(c) ||
        partitionKeyPredicatesToPushDown.isEmpty && !eqIndexedColumns.contains(c) &&
          !partitionKeyColumns.contains(c)
      p <- firstNonEmptySet(eqPredicatesByName(c), rangePredicatesByName(c))
    } yield p

    if (!inPredicateInPrimaryKey && eqIndexedColumns.nonEmpty)
      (eqIndexedPredicates ++ nonIndexedPredicates).toSet
    else
      Set.empty
  }

  /** Returns the set of predicates that can be safely pushed down to Cassandra */
  val predicatesToPushDown: Set[Predicate] =
    partitionKeyPredicatesToPushDown ++
      clusteringColumnPredicatesToPushDown ++
      indexedColumnPredicatesToPushDown

  /** Returns the set of predicates that cannot be pushed down to Cassandra,
    * so they must be applied by Spark  */
  val predicatesToPreserve: Set[Predicate] =
    predicates -- predicatesToPushDown
}
