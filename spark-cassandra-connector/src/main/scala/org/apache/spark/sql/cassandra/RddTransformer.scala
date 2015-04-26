package org.apache.spark.sql.cassandra

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources


import com.datastax.spark.connector.NamedColumnRef
import com.datastax.spark.connector.rdd.CassandraRDD

/**
 * Transform a RDD by filtering columns and pushdown source [[Filter]]s
 */
class RddTransformer(requiredColumns: Array[String], filters: Seq[Filter]) {

  /** CassandraRDD[CassandraSQLRow] is the only type supported for transferring */
  private type RDDType = CassandraRDD[CassandraSQLRow]

  /** Transfer selection to limit to columns specified */
  def maybeSelect(rdd: RDDType) : RDDType = {
    if (requiredColumns.nonEmpty) {
      rdd.select(requiredColumns.map(column => column: NamedColumnRef): _*)
    } else {
      rdd
    }
  }

  /** Push down predicates to Java driver query */
  private def maybePushdownPredicates(rdd: RDDType) : RDDType = {
    whereClause(filters) match {
      case (cql, values) if values.nonEmpty => rdd.where(cql, values: _*)
      case _ => rdd
    }
  }

  /** Construct Cql clause and retrieve the values from predicate */
  private def predicateToCqlAndValue(filter: Any): (String, Seq[Any]) = {
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

  /** Construct where clause from pushdown predicates */
  private def whereClause(pushdownPred: Seq[Any]): (String, Seq[Any]) = {
    val cqlValue = pushdownPred.map(predicateToCqlAndValue)
    val cql = cqlValue.map(_._1).mkString(" AND ")
    val args = cqlValue.flatMap(_._2)
    (cql, args)
  }

  /** Quote name */
  private def quoted(str: String): String = {
    "\"" + str + "\""
  }
  /** Transform rdd by applying selected columns and push downed predicates */
  def transform(rdd: RDDType) : RDDType = {
    maybePushdownPredicates(maybeSelect(rdd))
  }
}