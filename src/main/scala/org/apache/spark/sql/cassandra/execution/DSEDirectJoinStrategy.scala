package org.apache.spark.sql.cassandra.execution

import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.cassandra.{AlwaysOff, AlwaysOn, Automatic, CassandraSourceRelation}
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.{DataSourceScanExec, ProjectExec, SparkPlan}


/**
  * Direct Join Strategy
  * Converts logical plans where the join target is a Cassandra derived branch with joinWithCassandraTable
  * style Join
  */
case class DSEDirectJoinStrategy(spark: SparkSession) extends Strategy with Serializable {
  import DSEDirectJoinStrategy._

  val conf = spark.sqlContext.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if hasValidDirectJoin(joinType, leftKeys, rightKeys, condition, left, right) =>

      val (otherBranch, joinTargetBranch, joinKeys, buildType) = {
        if (leftValid(joinType, leftKeys, rightKeys, condition, left, right)) {
          (right, left, leftKeys, BuildLeft)
        } else {
          (left, right, rightKeys, BuildRight)
        }
      }

      /* We want to take advantage of all of our pushed filter code which happens in
      full table scans. Unfortunately the pushdown code itself is private within the
      DataSourceStrategy class. To work around this we will invoke DataSourceStrategy on
      our target branch. This will let us know all of the pushable filters that we can
      use in the direct join.
       */
      val dataSourceOpitimzedPlan = DataSourceStrategy(conf)(joinTargetBranch).head
      val cassandraScanExec = getScanExec(dataSourceOpitimzedPlan).get
      val cassandraRdd = getCassandraTableScanRDD(cassandraScanExec.execute()).get

      joinTargetBranch match {
        case PhysicalOperation(attributes, _, LogicalRelation(_: CassandraSourceRelation, _, _)) =>

          val directJoin =
            DSEDirectJoinExec(
            leftKeys,
            rightKeys,
            joinType,
            buildType,
            condition,
            planLater(otherBranch),
            aliasMap(attributes),
            cassandraRdd,
            cassandraScanExec
          )

          val newPlan = reorderPlan(dataSourceOpitimzedPlan, directJoin) :: Nil
          val newOutput = (newPlan.head.outputSet, newPlan.head.output.map(_.name))
          val oldOutput = (plan.outputSet, plan.output.map(_.name))
          val noMissingOutput = oldOutput._1.subsetOf(newPlan.head.outputSet)
          require(noMissingOutput, s"DSE DirectJoin Optimization produced invalid output. Original plan output: " +
            s"${oldOutput} was not part of ${newOutput} \nOld Plan\n${plan}\nNew Plan\n${newPlan}")

          newPlan
        case _ => Nil //Unable to do optimization on target branch
      }
    case _ => Nil //No valid Target for Join Optimization
  }

  def hasValidDirectJoin(
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: LogicalPlan,
    right: LogicalPlan): Boolean = {
    leftValid(joinType,leftKeys, rightKeys, condition, left, right) ||
      rightValid(joinType,leftKeys, rightKeys, condition, left, right)
  }

  /**
    * Checks whether the size of the cassandra target is larger than the
    * key plan by the sizeRatio specified in the configuration.
    */
  private def checkSizeRatio(cassandraPlan: LogicalPlan, keyPlan: LogicalPlan): Boolean = {
    val cassandraSource = getCassandraSource(cassandraPlan).get
    cassandraSource.directJoinSetting match {
      case AlwaysOn =>
        logDebug("No Size Test for Direct Join. Direct Join is Always On")
        true
      case AlwaysOff =>
        logDebug("Direct Join is Disabled")
        false
      case Automatic =>
        val ratio = BigDecimal(
          cassandraSource
            .sqlContext
            .getConf(DirectJoinSizeRatioParam.name, DirectJoinSizeRatioParam.default.toString))

        val cassandraSize = BigDecimal(cassandraPlan.stats(conf).sizeInBytes)
        val keySize = BigDecimal(keyPlan.stats(conf).sizeInBytes.doubleValue())

        logDebug(s"Checking if size ratio is good: $cassandraSize * $ratio > $keySize")

        if ((cassandraSize * ratio) > keySize) {
          true
        } else {
          false
        }
      }
  }

  def leftValid (joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: LogicalPlan,
    right: LogicalPlan): Boolean = (
    validJoinBranch(left, leftKeys)
      && validJoinType(BuildLeft, joinType)
      && checkSizeRatio(left, right)
    )

  def rightValid (joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: LogicalPlan,
    right: LogicalPlan): Boolean = (
    validJoinBranch(right, rightKeys)
      && validJoinType(BuildRight, joinType)
      && checkSizeRatio(right, left)
  )

}

object DSEDirectJoinStrategy extends Logging {

  /**
    * Recursively search the dependencies of an RDD for a CassandraTableScanRDD
    * Only searches single dependency chains, on branches chooses left
    */
  def getCassandraTableScanRDD(rdd: RDD[_]): Option[CassandraTableScanRDD[_]] ={
    rdd match {
      case cassandraTableScanRDD: CassandraTableScanRDD[_] =>
        Some(cassandraTableScanRDD)
      case other if rdd.dependencies.headOption.nonEmpty =>
        getCassandraTableScanRDD(other.dependencies.head.rdd)
      case _ => None
    }
  }

  /**
    * Returns a single logical source for the branch if there is one
    */
  def getLogicalRelation(plan: LogicalPlan): Option[LogicalRelation] = {
    val leaves = plan.collectLeaves
    if (leaves.size != 1) {
      None
    } else leaves.head match {
      case logicalRelation: LogicalRelation => Some(logicalRelation)
      case _ => None
    }
  }

  /**
    * Returns the single DataSourceScanExec for the branch if there is one
    */
  def getScanExec(plan: SparkPlan): Option[DataSourceScanExec] = {
     val leaves = plan.collectLeaves
    if (leaves.size != 1) {
      None
    } else leaves.head match {
      case dataSourceScanExec: DataSourceScanExec => Some(dataSourceScanExec)
      case _ => None
    }
  }

  /**
    * Currently we will only support Inner, LeftOuter and RightOuter joins
    * Depending on the side of the CassandraTarget different joins are allowed.
    */
  val validJoins: Map[BuildSide, Seq[JoinType]] = Map(
    BuildRight -> Seq[JoinType](Inner, LeftOuter),
    BuildLeft -> Seq[JoinType](Inner, RightOuter)
  )

  def validJoinType(cassandraSide: BuildSide, joinType: JoinType): Boolean =
    validJoins(cassandraSide).contains(joinType)

  /**
    * Checks whether there is only a single leaf to this plan and that the
    * leaf is a CassandraSourceRelation. If it is returns that relation.
    */
  def getCassandraSource(plan: LogicalPlan): Option[CassandraSourceRelation] = {
   getLogicalRelation(plan).flatMap(
     logicalSource => logicalSource.relation match {
       case cassandraSourceRelation: CassandraSourceRelation => Some(cassandraSourceRelation)
       case _ => None
     }
   )
  }

  /**
    * Checks whether a query plan has either a logical or physical node pulling data from cassandra
    */
  def hasCassandraChild[T <: QueryPlan[T]](plan: T): Boolean = {
    plan.children.size == 1 && plan.children.exists {
      case child: LogicalRelation if child.relation.isInstanceOf[CassandraSourceRelation] => true
      case child: DataSourceScanExec if child.relation.isInstanceOf[CassandraSourceRelation] => true
      case _ => false
    }
  }

  /**
    * Given our target Cassandra based branch, we remove the node which draws data
    * from cassandra (DataSourceScanExec) and replace it with the passed directJoin plan
    * instead.
    *
    * INPUT :
    *
    * //directJoin: DirectJoinPlan//
    * DirectJoin <-- GetKeyOperations
    *
    * //plan: Optimized CassandraScanBranch/
    * OtherOperations <-- CassandraScan
    *
    * OUTPUT:
    *
    * OtherOperations <-- DirectJoin <-- GetKeyOperations
    *
    * The output of (plan) is not changed, but its children should be
    * changed.
    *
    * This should only be called on optimized Physical Plans
    */
  def reorderPlan(plan: SparkPlan, directJoin: DSEDirectJoinExec): SparkPlan = {
    val reordered = plan match {
      //This may be the only node in the Plan
      case dataSourceScan: DataSourceScanExec
        if dataSourceScan.relation.isInstanceOf[CassandraSourceRelation] =>
        directJoin
      // Plan has children
      case normalPlan => normalPlan.transform{
        case penultimate if hasCassandraChild(penultimate) =>
          penultimate.withNewChildren(Seq(directJoin))
      }
    }

    /*
    Projections that lived on the Cassandra Branch will be missing column references to
    columns which are added by the join. We know these columns will either be added to the
    right or to the left of the current set of projection, so determine the missing side
    and add them.
     */
    reordered.transform{
       case p @ ProjectExec(projectList, child) =>
         val children = projectList.collect {
           case Alias(child: AttributeReference, _) => child.toAttribute
           case ref: AttributeReference => ref
         }
         val prefix = directJoin.output.takeWhile(attribute => !children.contains(attribute))
         val suffix = directJoin.output.reverse.takeWhile(attribute => !children.contains(attribute)).reverse
        ProjectExec (prefix ++ projectList ++ suffix, child)
    }
  }

  /**
    * Checks whether the Plan contains only acceptable logical nodes and all
    * partition keys are joined
    */
  def validJoinBranch(plan: LogicalPlan, keys: Seq[Expression]): Boolean = {
    val safePlan = containsSafePlans(plan)
    val pkConstrained = allPartitionKeysAreJoined(plan, keys)
    if (containsSafePlans(plan)){
      logDebug(s"Plan was safe")
    }
    if (pkConstrained) {
      logDebug(s"Plan constrained on all partition keys")
    }

    safePlan && pkConstrained
  }

  /**
    * Every partition key column must be a join key
    */
  def allPartitionKeysAreJoined(plan: LogicalPlan, joinKeys: Seq[Expression]): Boolean =
    plan match {
      case PhysicalOperation(
        attributes, _,
        LogicalRelation(cassandraSource: CassandraSourceRelation, _, _)) =>

        val joinKeyAliases =
          aliasMap(attributes)
            .filter{ case (_, value) => joinKeys.contains(value) }
        val partitionKeyNames = cassandraSource.tableDef.partitionKey.map(_.columnName)
        val allKeysPresent = partitionKeyNames.forall(joinKeyAliases.contains)

        if (!allKeysPresent) {
          logDebug(s"Not all $partitionKeyNames should be contained within $joinKeyAliases")
        }

        allKeysPresent
    case _ => false
  }

  /**
    * Map Source Names to Attributes
    */
  def aliasMap(aliases: Seq[NamedExpression]) = aliases.map {
    case a @ Alias(child: AttributeReference, _) => child.name -> a.toAttribute
    case namedExpression: NamedExpression => namedExpression.name -> namedExpression.toAttribute
  }.toMap

  /**
  * Checks whether a logical plan contains only Filters, Aliases
  * and CassandraSource Relations
  */
  def containsSafePlans(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(_, _, LogicalRelation(relation: CassandraSourceRelation, _ , _))
        if relation.directJoinSetting != AlwaysOff => true
      case _ => false
    }
  }



}
