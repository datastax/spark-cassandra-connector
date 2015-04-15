package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.NamedColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.DataType

/**
 * Transform a RDD by filtering [[[Attribute]] and pushdown Catalyst [[Expression]]s
 */
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

/**
 * Transform a RDD by filtering columns and pushdown source [[Filter]]s
 */
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


/**
 * Transform a RDD by filtering column selection and pushdown predicates
 */
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