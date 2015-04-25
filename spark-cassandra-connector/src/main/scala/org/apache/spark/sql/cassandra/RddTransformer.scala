package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.DataType


import com.datastax.spark.connector.NamedColumnRef
import com.datastax.spark.connector.rdd.CassandraRDD

/**
 * Transform a RDD by filtering [[[Attribute]] and pushdown Catalyst [[Expression]]s
 */
class CatalystRddTransformer(
    attributes: Seq[Attribute],
    pushdownPred: Seq[Expression])
  extends RddTransformer {

  override def maybeSelect(rdd: RDDType) : RDDType = {
    if (attributes.nonEmpty) {
      rdd.select(attributes.map(attribute => attribute.name: NamedColumnRef): _*)
    } else {
      rdd
    }
  }

  override protected def maybePushdownPredicates(rdd: RDDType) : RDDType = {
    whereClause(pushdownPred) match {
      case (cql, values) if values.nonEmpty => rdd.where(cql, values: _*)
      case _ => rdd
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

  override protected def predicateToCqlAndValue(predicate: Any): (String, Seq[Any]) = {
    predicate match {
      case cmp: BinaryComparison =>
        (quoted(cmp.references.head.name) + " " + predicateOperator(cmp) + " ?",
          Seq(castFromString(cmp.right.toString, cmp.right.dataType)))
      case in: In =>
        (quoted(in.value.references.head.name) + " IN " + in.list.map(_ => "?").mkString("(", ", ", ")"),
          in.list.map(value => castFromString(value.toString, value.dataType)))
      case inset: InSet =>
        (quoted(inset.value.references.head.name) + " IN " + inset.hset.toSeq.map(_ => "?").mkString("(", ", ", ")"),
          inset.hset.toSeq)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid predicate $predicate to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }

  private[this] def castFromString(value: String, dataType: DataType) : Any = {
    Cast(Literal(value), dataType).eval(null)
  }

}

/**
 * Transform a RDD by filtering columns and pushdown source [[Filter]]s
 */
class FilterRddTransformer(
    requiredColumns: Array[String],
    filters: Seq[Filter])
  extends RddTransformer {

  override def maybeSelect(rdd: RDDType) : RDDType = {
    if (requiredColumns.nonEmpty) {
      rdd.select(requiredColumns.map(column => column: NamedColumnRef): _*)
    } else {
      rdd
    }
  }

  override protected def maybePushdownPredicates(rdd: RDDType) : RDDType = {
    whereClause(filters) match {
      case (cql, values) if values.nonEmpty => rdd.where(cql, values: _*)
      case _ => rdd
    }
  }

  override protected def predicateToCqlAndValue(filter: Any): (String, Seq[Any]) = {
    filter match {
      case sources.EqualTo(attribute, value)            => (s"${quoted(attribute)} = ?", Seq(value))
      case sources.LessThan(attribute, value)           => (s"${quoted(attribute)} < ?", Seq(value))
      case sources.LessThanOrEqual(attribute, value)    => (s"${quoted(attribute)} <= ?", Seq(value))
      case sources.GreaterThan(attribute, value)        => (s"${quoted(attribute)} > ?", Seq(value))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quoted(attribute)} >= ?", Seq(value))
      case sources.In(attribute, values)                 =>
        (quoted(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), values.toSeq)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }
}


/**
 * Transform a RDD by filtering column selection and pushdown predicates
 */
abstract class RddTransformer extends Serializable {

  /** CassandraRDD[CassandraSQLRow] is the only type supported for transferring */
  protected type RDDType = CassandraRDD[CassandraSQLRow]

  /** Transfer selection to limit to columns specified */
  def maybeSelect(rdd: RDDType) : RDDType

  /** Push down predicates to Java driver query */
  protected def maybePushdownPredicates(rdd: RDDType) : RDDType

  /** Construct where clause from pushdown predicates */
  protected def whereClause(pushdownPred: Seq[Any]): (String, Seq[Any]) = {
    val cqlValue = pushdownPred.map(predicateToCqlAndValue)
    val cql = cqlValue.map(_._1).mkString(" AND ")
    val args = cqlValue.flatMap(_._2)
    (cql, args)
  }

  /** Construct Cql clause and retrieve the values from predicate */
  protected def predicateToCqlAndValue(predicate: Any): (String, Seq[Any])

  /** Quote name */
  protected def quoted(str: String): String = {
    "\"" + str + "\""
  }
  /** Transform rdd by applying selected columns and push downed predicates */
  def transform(rdd: RDDType) : RDDType = {
    maybePushdownPredicates(maybeSelect(rdd))
  }
}