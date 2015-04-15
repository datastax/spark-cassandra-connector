package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.NamedColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.DataType

class PredicatePushDown (predicates: Seq[Expression], tableDef: TableDef)
  extends PushDown(predicates: Seq[Expression], tableDef: TableDef) {

  def isEqualTo(predicate: Expression): Boolean = predicate.isInstanceOf[EqualTo]

  def isIn(predicate: Expression) : Boolean = predicate.isInstanceOf[In] || predicate.isInstanceOf[InSet]

  def isRangeComparison(predicate: Expression) : Boolean = predicate match {
    case _: LessThan           => true
    case _: LessThanOrEqual    => true
    case _: GreaterThan        => true
    case _: GreaterThanOrEqual => true
    case _                     => false
  }

  def isSingleColumn(predicate: Expression) = predicate.references.size == 1

  /** Returns the only column name referenced in the predicate */
  def predicateColumnName(predicate: Expression) : String = {
    require(predicate.references.size == 1, "Given predicate is not a single column predicate: " + predicate)
    predicate.references.head.name
  }
}

class FilterPushDown (predicates: Seq[Filter], tableDef: TableDef)
  extends PushDown(predicates: Seq[Filter], tableDef: TableDef) {

  def isEqualTo(filter: Filter): Boolean = filter.isInstanceOf[sources.EqualTo]

  def isIn(filter: Filter) : Boolean = filter.isInstanceOf[sources.In]

  def isRangeComparison(filter: Filter) : Boolean = filter match {
    case _: sources.LessThan           => true
    case _: sources.LessThanOrEqual    => true
    case _: sources.GreaterThan        => true
    case _: sources.GreaterThanOrEqual => true
    case _                             => false
  }

  def isSingleColumn(filter: Filter) = filter.isInstanceOf[sources.In] || filter.isInstanceOf[sources.EqualTo] ||
    filter.isInstanceOf[sources.LessThan] || filter.isInstanceOf[sources.LessThanOrEqual] ||
    filter.isInstanceOf[sources.GreaterThan] || filter.isInstanceOf[sources.GreaterThanOrEqual]


  /** Returns the only column name referenced in the predicate */
  def predicateColumnName(filter: Filter) : String = {
    filter match {
      case eq: sources.EqualTo            => eq.attribute
      case lt: sources.LessThan           => lt.attribute
      case le: sources.LessThanOrEqual    => le.attribute
      case gt: sources.GreaterThan        => gt.attribute
      case ge: sources.GreaterThanOrEqual => ge.attribute
      case in: sources.In                 => in.attribute
      case _ =>
        throw new UnsupportedOperationException(
          "It's not a valid filter to be pushed down, only >, <, >=, <= and In are allowed: " + filter)
    }
  }
}

/**
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
 *
 */
abstract class PushDown[T](predicates: Seq[T], tableDef: TableDef) {

  def isEqualTo(predicate: T): Boolean

  def isIn(predicate: T) : Boolean

  def isRangeComparison(predicate: T) : Boolean

  def isSingleColumn(predicate: T) : Boolean

  /** Returns the only column name referenced in the predicate */
  def predicateColumnName(predicate: T) : String

  private val partitionKeyColumns = tableDef.partitionKey.map(_.columnName)
  private val clusteringColumns = tableDef.clusteringColumns.map(_.columnName)
  private val indexedColumns = tableDef.allColumns.filter(_.isIndexedColumn).map(_.columnName)
  private val regularColumns = tableDef.regularColumns.map(_.columnName)
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
  private val partitionKeyPredicatesToPushDown: Seq[T] = {
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
  private val clusteringColumnPredicatesToPushDown: Seq[T] = {
    val (eqColumns, otherColumns) = clusteringColumns.span(eqPredicatesByName.contains)
    val eqPredicates = eqColumns.flatMap(eqPredicatesByName)
    val optionalNonEqPredicate = for {
      c <- otherColumns.headOption.toSeq
      p <- firstNonEmptySeq(rangePredicatesByName(c), inPredicatesByName(c).filter(
        _ => c==clusteringColumns.last))
    } yield p

    eqPredicates ++ optionalNonEqPredicate
  }

  /**
   * Selects indexed and regular column predicates for pushdown:
   * 1. At least one indexed column must be present in an equality predicate to be pushed down.
   * 2. Regular column predicates can be either equality or range predicates.
   * 3. If multiple predicates use the same column, equality predicates are preferred over range predicates.
   */
  private val indexedColumnPredicatesToPushDown: Seq[T] = {
    val inPredicateInPrimaryKey = partitionKeyPredicatesToPushDown.exists(isIn)
    val eqIndexedColumns = indexedColumns.filter(eqPredicatesByName.contains)
    val eqIndexedPredicates = eqIndexedColumns.flatMap(eqPredicatesByName)
    // Don't include partition predicates as None-indexed predicates if partition predicates can't
    // be pushed down because we use token range query which already has partition columns in the
    // where clause and it can't have other partial partition columns in where clause any more.
    val nonIndexedPredicates = for {
      c <- allColumns if !partitionKeyPredicatesToPushDown.isEmpty && !eqIndexedColumns.contains(c) ||
      partitionKeyPredicatesToPushDown.isEmpty && !eqIndexedColumns.contains(c) &&
        !partitionKeyColumns.contains(c)
      p <- firstNonEmptySeq(eqPredicatesByName(c), rangePredicatesByName(c))
    } yield p

    if (!inPredicateInPrimaryKey && eqIndexedColumns.nonEmpty)
      eqIndexedPredicates ++ nonIndexedPredicates
    else
      Nil
  }

  val toPushDown = (
    partitionKeyPredicatesToPushDown ++
      clusteringColumnPredicatesToPushDown ++
      indexedColumnPredicatesToPushDown).distinct

  val toPreserve = predicates.filterNot(toPushDown.toSet.contains)
}

class RddPredicateSelectPdTrf(attributes: Seq[Attribute], columnNameByLowercase: Map[String, String],
                              pushdownPred: Seq[Expression]) extends RddSelectPdTrf {

  override var maybeSelect = if (attributes.nonEmpty) { rdd: RDDType =>
    rdd.select(attributes.map(a => columnNameByLowercase(a.name): NamedColumnRef): _*)
  } else { rdd: RDDType =>
    rdd
  }

  override protected var maybePushdownPredicates = whereClause(pushdownPred) match {
    case (cql, values) if values.nonEmpty => rdd: RDDType => rdd.where(cql, values: _*)
    case _ => rdd: RDDType => rdd
  }


  override protected def predicateRhsValue(predicate: Any): Seq[Any] = {
    predicate match {
      case cmp: BinaryComparison => Seq(castFromString(cmp.right.toString, cmp.right.dataType))
      case in: In => in.list.map(value => castFromString(value.toString, value.dataType))
      case inset: InSet => inset.hset.toSeq
      case _ => throw new UnsupportedOperationException("Unsupported predicate: " + predicate)
    }
  }

  private[this] def predicateOperator(predicate: Expression): String = {
    predicate match {
      case _: EqualTo =>            "="
      case _: LessThan =>           "<"
      case _: LessThanOrEqual =>    "<="
      case _: GreaterThan =>        ">"
      case _: GreaterThanOrEqual => ">="
      case _: In | _: InSet =>      "IN"
      case _ => throw new UnsupportedOperationException(
        "It's not a valid predicate to be pushed down, only >, <, >=, <= and In are allowed: " + predicate)
    }
  }

  override protected def predicateToCql(predicate: Any): String = {
    predicate match {
      case cmp: BinaryComparison =>
        cmp.references.head.name + " " + predicateOperator(cmp) + " ?"
      case in: In =>
        in.value.references.head.name + " IN " + in.list.map(_ => "?").mkString("(", ", ", ")")
      case inset: InSet =>
        inset.value.references.head.name + " IN " + inset.hset.toSeq.map(_ => "?").mkString("(", ", ", ")")
      case _ =>
        throw new UnsupportedOperationException(
          "It's not a valid predicate to be pushed down, only >, <, >=, <= and In are allowed: " + predicate)
    }
  }

  private[this] def castFromString(value: String, dataType: DataType) = Cast(Literal(value), dataType).eval(null)

}

class RddFilterSelectPdTrf(requiredColumns: Array[String], columnNameByLowercase: Map[String, String],
                           filters: Seq[Filter]) extends RddSelectPdTrf {

  override var maybeSelect = if (requiredColumns.nonEmpty) { rdd: RDDType =>
    rdd.select(requiredColumns.map(a => columnNameByLowercase(a): NamedColumnRef): _*)
  } else { rdd: RDDType =>
    rdd
  }

  override protected var maybePushdownPredicates = whereClause(filters) match {
    case (cql, values) if values.nonEmpty => rdd: RDDType => rdd.where(cql, values: _*)
    case _ => rdd: RDDType => rdd
  }


  override protected def predicateRhsValue(filter: Any): Seq[Any] = {
    filter match {
      case eq: sources.EqualTo            => Seq(eq.value)
      case lt: sources.LessThan           => Seq(lt.value)
      case le: sources.LessThanOrEqual    => Seq(le.value)
      case gt: sources.GreaterThan        => Seq(gt.value)
      case ge: sources.GreaterThanOrEqual => Seq(ge.value)
      case in: sources.In                 => in.values.toSeq
      case _ => throw new UnsupportedOperationException("Unsupported filter: " + filter)
    }
  }

  private[this] def filterOperator(filter: Filter): String = {
    filter match {
      case _: sources.EqualTo            => "="
      case _: sources.LessThan           => "<"
      case _: sources.LessThanOrEqual    => "<="
      case _: sources.GreaterThan        => ">"
      case _: sources.GreaterThanOrEqual => ">="
      case _: sources.In                 => "IN"
      case _ => throw new UnsupportedOperationException(
        "It's not a valid filter to be pushed down, only >, <, >=, <= and In are allowed: " + filter)
    }
  }

  override protected def predicateToCql(filter: Any): String = {
    filter match {
      case eq: sources.EqualTo            => eq.attribute + " " + filterOperator(eq) + " ?"
      case lt: sources.LessThan           => lt.attribute + " " + filterOperator(lt) + " ?"
      case le: sources.LessThanOrEqual    => le.attribute + " " + filterOperator(le) + " ?"
      case gt: sources.GreaterThan        => gt.attribute + " " + filterOperator(gt) + " ?"
      case ge: sources.GreaterThanOrEqual => ge.attribute + " " + filterOperator(ge) + " ?"
      case in: sources.In                 =>
        in.attribute + " IN " + in.values.map(_ => "?").mkString("(", ", ", ")")
      case _ =>
        throw new UnsupportedOperationException(
          "It's not a valid filter to be pushed down, only >, <, >=, <= and In are allowed: " + filter)
    }
  }

}

abstract class RddSelectPdTrf extends Serializable {

  protected type RDDType = CassandraRDD[CassandraSQLRow]

  var maybeSelect : RDDType => RDDType

  protected var maybePushdownPredicates : RDDType => RDDType

  protected def whereClause(pushdownPred: Seq[Any]): (String, Seq[Any]) = {
    val cql = pushdownPred.map(predicateToCql).mkString(" AND ")
    val args = pushdownPred.flatMap(predicateRhsValue)
    (cql, args)
  }

  protected def predicateToCql(predicate: Any): String
  protected def predicateRhsValue(predicate: Any): Seq[Any]

  def transform(rdd: RDDType) : RDDType = {
    maybeSelect andThen maybePushdownPredicates apply rdd
  }
}