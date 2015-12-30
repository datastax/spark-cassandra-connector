package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter

/** A unified API for predicates, used by [[BasicCassandraPredicatePushDown]].
  *
  * Keeps all the Spark-specific stuff out of `BasicCassandraPredicatePushDown`
  * It is also easy to plug-in custom predicate implementations for unit-testing.
  */
trait PredicateOps[Predicate] {

  /** Returns the column name of the only column in the predicate.
    * Will throw `IllegalArgumentException` if predicate is not a single-column predicate. */
  def columnName(p: Predicate): String

  /** Returns true if the predicate restricts exactly one column. */
  def isSingleColumnPredicate(p: Predicate): Boolean

  /** Returns true for predicates of type: column = value */
  def isEqualToPredicate(p: Predicate): Boolean

  /** Returns true for predicates of type: column (< | <= | >= | >) value */
  def isRangePredicate(p: Predicate): Boolean

  /** Returns true for predicates of type: column IN (value1, value2, ...) */
  def isInPredicate(p: Predicate): Boolean
}

/** Provides `PredicateOps` adapters for Expression and Filter classes */
object PredicateOps {

  /** Adapts the API of Catalyst `Expression` to the API expected by `BasicCassandraPredicatePushDown` */
  implicit object ExpressionOps extends PredicateOps[Expression] {

    override def columnName(p: Expression): String = {
      require(isSingleColumnPredicate(p), s"Not a single-column predicate: $p")
      p.references.head.name
    }

    override def isSingleColumnPredicate(p: Expression): Boolean =
      p.references.size == 1

    override def isRangePredicate(p: Expression): Boolean = p match {
      case _: expressions.LessThan => true
      case _: expressions.LessThanOrEqual => true
      case _: expressions.GreaterThan => true
      case _: expressions.GreaterThanOrEqual => true
      case _ => false
    }

    override def isEqualToPredicate(p: Expression): Boolean =
      p.isInstanceOf[expressions.EqualTo]

    override def isInPredicate(p: Expression): Boolean =
      p.isInstanceOf[expressions.In] || p.isInstanceOf[expressions.InSet]

  }

  /** Adapts the API of Catalyst `Filter` to the API expected by `BasicCassandraPredicatePushDown` */
  implicit object FilterOps extends PredicateOps[Filter] {

    override def columnName(p: Filter): String = p match {
      case eq: sources.EqualTo => eq.attribute
      case lt: sources.LessThan => lt.attribute
      case lte: sources.LessThanOrEqual => lte.attribute
      case gt: sources.GreaterThan => gt.attribute
      case gte: sources.GreaterThanOrEqual => gte.attribute
      case in: sources.In => in.attribute
      case _ => throw new IllegalArgumentException(
        s"Don't know how to get column name from the predicate: $p")
    }

    override def isSingleColumnPredicate(p: Filter): Boolean =
      isRangePredicate(p) || isEqualToPredicate(p) || isInPredicate(p)

    override def isRangePredicate(p: Filter): Boolean = p match {
      case _: sources.LessThan => true
      case _: sources.LessThanOrEqual => true
      case _: sources.GreaterThan => true
      case _: sources.GreaterThanOrEqual => true
      case _ => false
    }

    override def isEqualToPredicate(p: Filter): Boolean =
      p.isInstanceOf[sources.EqualTo]

    override def isInPredicate(p: Filter): Boolean =
      p.isInstanceOf[sources.In]
  }
}