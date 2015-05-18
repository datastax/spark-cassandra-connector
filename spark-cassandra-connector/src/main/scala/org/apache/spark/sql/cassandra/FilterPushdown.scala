package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter


/**
 * Calculate the pushdown filters for a given table and filters.
 *
 * Partition pruning filters are also detected an applied.
 *  1. Only push down no-partition key column filters with =, >, <, >=, <= filter
 *  2. Only push down primary key column filters with = or IN filter.
 *  3. If there are regular columns in the pushdown filters, they should have
 *     at least one EQ expression on an indexed column and no IN filters.
 *  4. All partition column filters must be included in the filters to be pushed down,
 *     only the last part of the partition key can be an IN filter. For each partition column,
 *     only one filter is allowed.
 *  5. For cluster column filters, only last filter can be non-EQ filter
 *     including IN filter, and preceding column filters must be EQ filters.
 *     If there is only one cluster column filter, the filters could be any non-IN filter.
 *  6. There is no pushdown filters if there is any OR condition or NOT IN condition.
 *  7. We're not allowed to push down multiple filters for the same column if any of them
 *     is equality or IN filter.
 *
 */
object FilterPushdown {

  def pushdown(filters: Seq[Filter], tableDef: TableDef) : Seq[Filter] = {

    val partitionKeyColumns = tableDef.partitionKey.map(_.columnName)
    val clusteringColumns = tableDef.clusteringColumns.map(_.columnName)
    val indexedColumns = tableDef.allColumns.filter(_.isIndexedColumn).map(_.columnName)
    val regularColumns = tableDef.regularColumns.map(_.columnName)
    val allColumns = partitionKeyColumns ++ clusteringColumns ++ regularColumns

    val singleColumnFilters = filters.collect(isSingleColumnFilter)

    val eqFilters = singleColumnFilters.collect({case filter: sources.EqualTo => filter})
    val eqFiltersByName = eqFilters.groupBy(filterColumnName)
      .mapValues(_.take(1))       // take(1) in order not to push down more than one EQ filter for the same column
      .withDefaultValue(Seq.empty)

    val inFilters = singleColumnFilters.collect({case filter: sources.In => filter})
    val inFiltersByName = inFilters.groupBy(filterColumnName)
      .mapValues(_.take(1))      // take(1) in order not to push down more than one IN filter for the same column
      .withDefaultValue(Seq.empty)

    val rangeFilters = singleColumnFilters.collect(isRangeComparisonFilter)
    val rangeFiltersByName = rangeFilters.groupBy(filterColumnName).withDefaultValue(Seq.empty)

    /** Returns a first non-empty sequence. If not found, returns an empty sequence. */
    def firstNonEmptySeq[Type](sequences: Seq[Type]*): Seq[Type] =
    sequences.find(_.nonEmpty).getOrElse(Seq.empty[Type])

    /**
     * Selects partition key filters for pushdown:
     * 1. Partition key filters must be equality or IN filters.
     * 2. Only the last partition key column filter can be an IN.
     * 3. All partition key filters must be used or none.
     */
    val partitionKeyFiltersToPushDown: Seq[Filter] = {
      val (eqColumns, otherColumns) = partitionKeyColumns.span(eqFiltersByName.contains)
      val inColumns = otherColumns.headOption.toSeq.filter(inFiltersByName.contains)
      if (eqColumns.size + inColumns.size == partitionKeyColumns.size)
        eqColumns.flatMap(eqFiltersByName) ++ inColumns.flatMap(inFiltersByName)
      else
        Nil
    }

    /**
     * Selects clustering key filters for pushdown:
     * 1. Clustering column filters must be equality filters, except the last one.
     * 2. The last filter is allowed to be an equality or a range filter.
     * 3. The last filter is allowed to be an IN filter only if it was preceded by all other equality filters.
     * 4. Consecutive clustering columns must be used, but, contrary to partition key, the tail can be skipped.
     */
    val clusteringColumnFiltersToPushDown: Seq[Filter] = {
      val (eqColumns, otherColumns) = clusteringColumns.span(eqFiltersByName.contains)
      val eqFilters = eqColumns.flatMap(eqFiltersByName)
      val optionalNonEqFilter = for {
        c <- otherColumns.headOption.toSeq
        p <- firstNonEmptySeq(rangeFiltersByName(c), inFiltersByName(c).filter(
          _ => c==clusteringColumns.last))
      } yield p

      eqFilters ++ optionalNonEqFilter
    }

    /**
     * Selects indexed and regular column filters for pushdown:
     * 1. At least one indexed column must be present in an equality filter to be pushed down.
     * 2. Regular column filters can be either equality or range filters.
     * 3. If multiple filters use the same column, equality filters are preferred over range filters.
     */
    val indexedColumnFiltersToPushDown: Seq[Filter] = {
      val inFilterInPrimaryKey = partitionKeyFiltersToPushDown.exists(isInFilter)
      val eqIndexedColumns = indexedColumns.filter(eqFiltersByName.contains)
      val eqIndexedFilters = eqIndexedColumns.flatMap(eqFiltersByName)
      // Don't include partition filters as None-indexed filters if partition filters can't
      // be pushed down because we use token range query which already has partition columns in the
      // where clause and it can't have other partial partition columns in where clause any more.
      val nonIndexedFilters = for {
        c <- allColumns if partitionKeyFiltersToPushDown.nonEmpty &&
           !eqIndexedColumns.contains(c) ||
          partitionKeyFiltersToPushDown.isEmpty &&
            !eqIndexedColumns.contains(c) &&
            !partitionKeyColumns.contains(c)
        p <- firstNonEmptySeq(eqFiltersByName(c), rangeFiltersByName(c))
      } yield p

      if (!inFilterInPrimaryKey && eqIndexedColumns.nonEmpty)
        eqIndexedFilters ++ nonIndexedFilters
      else
        Nil
    }

    (partitionKeyFiltersToPushDown ++
      clusteringColumnFiltersToPushDown ++
      indexedColumnFiltersToPushDown).distinct
  }

  /** Check if the filter is In filter */
  private def isInFilter(filter: Filter) : Boolean = filter match {
    case _: sources.EqualTo => true
    case _                  => false
  }

  /** Check if the filter is a range comparison filter */
  private def isRangeComparisonFilter: PartialFunction[Filter, Filter] = {
    case lt: sources.LessThan => lt
    case le: sources.LessThanOrEqual => le
    case gt: sources.GreaterThan => gt
    case ge: sources.GreaterThanOrEqual => ge
  }

  /** Check if the column is a single column filter */
  private def isSingleColumnFilter: PartialFunction[Filter, Filter] = {
    case eq: sources.EqualTo => eq
    case in: sources.In => in
    case otherFilter => isRangeComparisonFilter(otherFilter)
  }

  /** Returns the only column name referenced in the filter */
  private def filterColumnName(filter: Filter) : String = filter match {
    case eq: sources.EqualTo            => eq.attribute
    case lt: sources.LessThan           => lt.attribute
    case le: sources.LessThanOrEqual    => le.attribute
    case gt: sources.GreaterThan        => gt.attribute
    case ge: sources.GreaterThanOrEqual => ge.attribute
    case in: sources.In                 => in.attribute
    case _ =>
      throw new UnsupportedOperationException(
        s"filter $filter is not valid to be pushed down, only >, <, >=, <= and In are allowed.")
  }

}

