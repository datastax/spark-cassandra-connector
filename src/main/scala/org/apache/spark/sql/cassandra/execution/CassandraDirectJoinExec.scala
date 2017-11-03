package org.apache.spark.sql.cassandra.execution

import com.datastax.spark.connector.{ColumnName, SomeColumns}
import com.datastax.spark.connector.rdd.{CassandraJoinRDD, CassandraLeftJoinRDD, CassandraTableScanRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.execution.unsafe.{UnsafeRowReaderFactory, UnsafeRowWriterFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BindReferences, EqualTo, Expression, GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildSide}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
  * A physical plan for performing a join against a CassandraTable given a set of keys.
  */
case class CassandraDirectJoinExec(
  leftKeys: Seq[Expression],
  rightKeys: Seq[Expression],
  joinType: JoinType,
  cassandraSide: BuildSide,
  condition: Option[Expression],
  child: SparkPlan,
  aliasMap: Map[String, Attribute],
  cassandraScan: CassandraTableScanRDD[_],
  cassandraPlan: DataSourceScanExec) extends UnaryExecNode {

  val numOutputRows = SQLMetrics.createMetric(sparkContext, "number of output rows")

  override lazy val metrics = Map(
    "numOutputRows" -> numOutputRows
  )

  val keySource = child

  val keyspace = cassandraScan.tableDef.keyspaceName
  val table = cassandraScan.tableDef.tableName
  val whereClause = cassandraScan.where
  val readConf = cassandraScan.readConf
  val selectedColumns = cassandraScan.selectedColumnRefs
  val primaryKeys = cassandraScan.tableDef.primaryKey.map(_.columnName)
  val cassandraSchema = cassandraPlan.schema

  val attributeToCassandra = aliasMap.map(_.swap)

  val leftJoinCouplets =
    if (cassandraSide == BuildLeft) leftKeys.zip(rightKeys) else rightKeys.zip(leftKeys)

  /*
    * Sort out the Equivalence clauses which we are going to use with Cassandra from those
    * which need to be handled manually. For example we can handle a clause that compares
    * a value to a partition key in cassandra but we can't handle an equality clause between
    * a value and a non partition key column.
    */
  val (pkJoinCoulplets, otherJoinCouplets) = leftJoinCouplets.partition {
    case (cassandraAttribute: Attribute, _) =>
      attributeToCassandra.get(cassandraAttribute) match {
        case Some(name) if primaryKeys.contains(name) => true
        case _ => false
    }
    case _ => false
  }

  val (joinColumns, joinExpressions) = pkJoinCoulplets.map { case (cAttr: Attribute, otherCol: Expression) =>
    (ColumnName(attributeToCassandra(cAttr)), BindReferences.bindReference(otherCol, keySource.output))
  }.unzip

  /**
    * We need to cover both the conditions that haven't be specified in the equi join and those
    * in the equi join which are not partition key conditions. We do this by building up a new
    * condition which we will apply to rows coming out of the join
    */
  @transient private[this] lazy val boundCondition = {
    val unhandledEquiPredicates = otherJoinCouplets
      .map( otherEqui => EqualTo(otherEqui._1, otherEqui._2))
      .reduceOption(And)

    val unhandledConditions = Seq(unhandledEquiPredicates, condition).flatten.reduceOption(And)

    if (unhandledConditions.isDefined) {
      newPredicate(unhandledConditions.get , keySource.output ++ cassandraPlan.output).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  val (left, right) = if (cassandraSide == BuildLeft) {
    (cassandraPlan, keySource)
  } else {
    (keySource, cassandraPlan)
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(s"CassandraDirectJoin should not take $x as the JoinType")
    }
  }

  protected def createResultProjection(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream (Key source) side on left to simplify implementation
      UnsafeProjection.create(
        output, (keySource.output ++ cassandraPlan.output).map(_.withNullability(true)))
  }

  /**
    * Join type do the correct JoinWithCassandraTable Operation
    */
  override protected def doExecute(): RDD[InternalRow] = {
    /* UnsafeRows are pointers to spots in memory and when our
     * UnsafeProject is called on the next element it rewrites our first
     * pointer. Since we call our executions async we end up losing
     * the pointer to the join key unless we make a copy of the pointer
     *
     *
     * see @UnsafeRow.copy()
     * see @UnsafeProjection
     */
    val unsafeKeyRows: RDD[UnsafeRow] = keySource
        .execute()
        .mapPartitions(it => {
          val projection = UnsafeProjection.create(keySource.schema)
          it.map(row => projection.apply(row).copy())
        })

    implicit val rwf = new UnsafeRowWriterFactory(joinExpressions)
    implicit val rrf = new UnsafeRowReaderFactory(cassandraSchema)

    def innerJoin() = {
      val joinRDD = new CassandraJoinRDD[UnsafeRow, UnsafeRow](
        unsafeKeyRows,
        keyspace,
        table,
        cassandraScan.connector,
        cassandraScan.columnNames,
        SomeColumns(joinColumns: _*),
        cassandraScan.where,
        cassandraScan.limit,
        cassandraScan.clusteringOrder,
        cassandraScan.readConf)

      val joinRow = new JoinedRow
      joinRDD.mapPartitions { it =>
        val resultProjection = createResultProjection
        it.map { case (unsafeKeyRow, cassandraRow) =>
          numOutputRows.add(1)
          joinRow.withLeft(unsafeKeyRow)
          joinRow.withRight(cassandraRow)
          resultProjection(joinRow)
        }.filter(boundCondition)
      }
    }

    def outerJoin() = {
      val joinRDD = new CassandraLeftJoinRDD[UnsafeRow, UnsafeRow](
        unsafeKeyRows,
        keyspace,
        table,
        cassandraScan.connector,
        cassandraScan.columnNames,
        SomeColumns(joinColumns: _*),
        cassandraScan.where,
        cassandraScan.limit,
        cassandraScan.clusteringOrder,
        cassandraScan.readConf)

      val joinRow = new JoinedRow
      joinRDD.mapPartitions { it =>
        val resultProjection = createResultProjection
        val nullRow = new GenericInternalRow(cassandraPlan.output.length)
        it.map { case (unsafeKeyRow, cassandraRow) =>
          numOutputRows.add(1)
          joinRow.withLeft(unsafeKeyRow)
          joinRow.withRight(cassandraRow.getOrElse(nullRow))
          resultProjection(joinRow)
        }.filter(boundCondition)

      }
    }

    if (Seq(RightOuter, LeftOuter).contains(joinType)) {
      outerJoin()
    } else {
      innerJoin()
    }
  }


  override def simpleString: String = {
    val pushedWhere =
      whereClause
        .predicates.zip(whereClause.values)
        .filter(_._1.nonEmpty)
        .map{case (where, value) => s"($where:$value)"}
        .mkString(" Pushed {", ", ", "}")

    val selectString = selectedColumns.mkString("Reading (", ", ", ")")

    val joinString = pkJoinCoulplets
      .map{ case (colref: Attribute, exp) => s"${attributeToCassandra(colref)} = ${exp}"}
      .mkString(", ")

    s"DSE Join[${joinString}] $keyspace.$table - $selectString${pushedWhere} "
  }
}
