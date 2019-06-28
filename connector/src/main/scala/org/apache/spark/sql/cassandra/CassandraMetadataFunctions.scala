package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SparkSession}

trait CassandraMetadataFunction extends UnaryExpression with Unevaluable {
  def confParam: String

  def cql: String = confParam.toUpperCase

  def isCollectionType(dataType: DataType) : Boolean = {
    dataType match {
      case _: ArrayType => true
      case _: MapType => true
      case _ => false
    }
  }
}

case class CassandraTTL(child: Expression) extends CassandraMetadataFunction {
  override def nullable: Boolean = false

  override def sql: String = s"TTL(${child.sql})"

  override def dataType: DataType = IntegerType

  override def confParam: String = CassandraSourceRelation.TTLParam.name
}

case class CassandraWriteTime(child: Expression) extends CassandraMetadataFunction {
  override def nullable: Boolean = false

  override def sql: String = s"WRITETIME(${child.sql})"

  override def dataType: DataType = LongType

  override def confParam: String = CassandraSourceRelation.WriteTimeParam.name
}

object CassandraMetadataFunction {

  def registerMetadataFunctions(session: SparkSession): Unit = {
    session.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier("ttl"),
      CassandraMetadataFunction.cassandraTTLFunctionBuilder)
    session.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier("writetime"),
      CassandraMetadataFunction.cassandraWriteTimeFunctionBuilder)
  }

  def cassandraTTLFunctionBuilder(args: Seq[Expression]) = {
    if (args.length != 1) {
      throw new AnalysisException(s"Unable to call Cassandra ttl with more than 1 argument, given" +
        s" $args")
    }
    CassandraTTL(args.head)
  }

  def cassandraWriteTimeFunctionBuilder(args: Seq[Expression]) = {
    if (args.length != 1) {
      throw new AnalysisException(s"Unable to call Cassandra writetime with more than 1 argument," +
        s" given $args")
    }
    CassandraWriteTime(args.head)
  }
}

object CassandraMetaDataRule extends Rule[LogicalPlan] {

  def replaceMetadata(metaDataExpression: CassandraMetadataFunction, plan: LogicalPlan)
  : LogicalPlan = {
    assert(metaDataExpression.child.isInstanceOf[AttributeReference],
      s"""Can only use Cassandra Metadata Functions on Attribute References,
         |found a ${metaDataExpression.child.getClass}""".stripMargin)

    (metaDataExpression.child.isInstanceOf[AttributeReference])

    val cassandraColumnName = metaDataExpression.child.asInstanceOf[AttributeReference].name
    val cassandraParameter = s"${metaDataExpression.confParam}.$cassandraColumnName"
    val cassandraCql = s"${metaDataExpression.cql}($cassandraColumnName)"

    val cassandraRelation = plan.collectFirst{
      case plan@LogicalRelation(relation: CassandraSourceRelation, _, _, _)
        if relation.tableDef.columnByName.contains(cassandraColumnName) => relation }
      .getOrElse(throw new IllegalArgumentException(
        s"Unable to find Cassandra Source Relation for TTL/Writetime for column $cassandraColumnName"))

    val columnDef = cassandraRelation.tableDef.columnByName(cassandraColumnName)

    val newAttributeReference = if (columnDef.isMultiCell) {
        AttributeReference(cassandraCql, ArrayType(metaDataExpression.dataType), true)()
      } else {
        AttributeReference(cassandraCql, metaDataExpression.dataType, true)()
      }

    // Remove Metadata Expressions
    val metadataFunctionRemovedPlan = plan.transformAllExpressions {
      case expression: Expression if expression == metaDataExpression => newAttributeReference
    }

    // Add Metadata to CassandraSource
    metadataFunctionRemovedPlan.transform {
      case plan@LogicalRelation(relation: CassandraSourceRelation, _, _, _)
        if relation.tableDef.columnByName.contains(cassandraColumnName) =>
        val modifiedCassandraRelation = relation.copy(sparkConf = relation.sparkConf.clone()
          .set(cassandraParameter, cassandraCql))
        plan.copy(relation = modifiedCassandraRelation, output = plan.output :+ newAttributeReference)
    }
  }

  def findMetadataExpressions(logicalPlan: LogicalPlan): Seq[CassandraMetadataFunction] = {
    def findMetadataExpressions(expressions: Seq[Expression]): Seq[CassandraMetadataFunction] = {
      expressions.collect{
        case metadata: CassandraMetadataFunction => Seq(metadata)
        case parent: Expression => findMetadataExpressions(parent.children)
      }.flatten
    }
    findMetadataExpressions(logicalPlan.expressions)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case planWithMetaData: LogicalPlan if findMetadataExpressions(planWithMetaData).nonEmpty =>
        val metadataExpressions = findMetadataExpressions(planWithMetaData)
        metadataExpressions.foldLeft[LogicalPlan](planWithMetaData) {
          case (plan, expression) => replaceMetadata(expression, plan)
        }
    }
  }
}
