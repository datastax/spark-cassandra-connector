package org.apache.spark.sql.cassandra

import com.datastax.spark.connector._
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.execution.LeafNode

import com.datastax.spark.connector.mapper.IndexedByNameColumnRef

@DeveloperApi
case class CassandraTableScan(
  attributes: Seq[Attribute],
  relation: CassandraRelation,
  pushdownPred: Seq[Expression])(
  @transient val context: CassandraSQLContext)
  extends LeafNode with Logging{

  private def inputRdd = {
    logInfo(s"attributes : ${attributes.map(_.name).mkString(",")}")
    //TODO: cluster level CassandraConnector, read configuration settings
    var rdd = context.sparkContext.cassandraTable[CassandraSQLRow](relation.keyspaceName, relation.tableName)
    if (attributes.map(_.name).size > 0)
      rdd = rdd.select(attributes.map(a => relation.columnNameByLowercase(a.name): IndexedByNameColumnRef): _*)
    if (pushdownPred.nonEmpty) {
      val(cql, values) = whereClause(pushdownPred)
      rdd = rdd.where(cql, values: _*)
    }

    rdd
  }

  private[this] def whereClause(pushdownPred: Seq[Expression]) : (String, Seq[Any]) = {
    val cql = pushdownPred.map(predicateToCql).mkString(" AND ")
    val args = pushdownPred.flatMap(predicateRhsValue)
    (cql, args)
  }

  private[this] def predicateRhsValue(predicate: Expression): Seq[Any] = {
    predicate match {
      case cmp: BinaryComparison => Seq(castFromString(cmp.right.toString, cmp.right.dataType))
      case in: In => in.list.map(value => castFromString(value.toString, value.dataType))
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
      case _: In =>                 "IN"
      case _ => throw new UnsupportedOperationException(
        "It's not a valid predicate to be pushed down, only >, <, >=, <= and In are allowed: " + predicate)
    }
  }

  private[this] def predicateToCql(predicate: Expression): String = {
    predicate match {
      case cmp: BinaryComparison =>
        cmp.references.head.name + " " + predicateOperator(cmp) + " ?"
      case in: In =>
        in.value.references.head.name + " IN " + in.list.map(_ => "?").mkString("(", ", ", ")")
      case _ =>
        throw new UnsupportedOperationException(
          "It's not a valid predicate to be pushed down, only >, <, >=, <= and In are allowed: " + predicate)
    }
  }

  private[this] def castFromString(value: String, dataType: DataType) = Cast(Literal(value), dataType).eval(null)

  /**
   * Runs this query returning the result as an RDD[Row].
   * cast CassandraRDD to RDD[ROW]
   */
  override def execute() = inputRdd.asInstanceOf[RDD[Row]]
  override def output = if (attributes.isEmpty) relation.output else attributes

}
