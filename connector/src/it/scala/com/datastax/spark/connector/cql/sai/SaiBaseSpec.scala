package com.datastax.spark.connector.cql.sai

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.SparkCassandraITSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.{FilterExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.sources.{Filter, IsNotNull}
import org.scalatest.Matchers

trait SaiBaseSpec extends Matchers with SparkCassandraITSpecBase {
  override lazy val conn = CassandraConnector(defaultConf)

  def df(table: String): DataFrame = spark.read.cassandraFormat(table, ks).load()

  def createTableWithIndexes(session: CqlSession, tableName: String, columns: Seq[String]): Unit = {
    val typesDeclaration = columns.map { t => s"${t}_col ${t}" }.mkString("", ",", ",")

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$tableName (
         |  pk int,
         |  $typesDeclaration
         |  PRIMARY KEY (pk));""".stripMargin)

    columns.foreach { t =>
      session.execute(
        s"CREATE CUSTOM INDEX ${t}_sai_idx ON $ks.$tableName (${t}_col) USING 'StorageAttachedIndex';")
    }
  }

  def findFilterOption(plan: SparkPlan): Option[FilterExec] = {
    plan match {
      case filter: FilterExec => Option(filter)
      case _ => None
    }
  }

  def findFilter(plan: SparkPlan): FilterExec = {
    findFilterOption(plan).getOrElse(throw new NoSuchElementException("Filter was not found in the given plan"))
  }

  def findDataSource(plan: SparkPlan): RowDataSourceScanExec = {
    plan match {
      case filter: FilterExec => findDataSource(filter.child)
      case ds: RowDataSourceScanExec => ds
      case _ => throw new NoSuchElementException("RowDataSourceScanExec was not found in the given plan")
    }
  }

  def debug(dataFrame: DataFrame)(f: => Unit): DataFrame = {
    dataFrame.explain(true)
    f
    dataFrame.show()
    dataFrame
  }

  def assertPushDown(dataFrame: DataFrame): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    withClue("The given plan should not contain Filter element, some of the predicates were not pushed down.") {
      findFilterOption(plan) should not be defined
    }
  }

  def assertNoPushDown(dataFrame: DataFrame): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    findFilter(plan)
    val source = findDataSource(plan)
    withClue("The given plan should not contain pushed down predicates") {
      source.handledFilters.filterNot(_.isInstanceOf[IsNotNull]) shouldBe empty
    }
  }

  def assertNonPushedColumns(dataFrame: DataFrame, nonPushedColumns: String*): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    val filter = findFilter(plan)
    val nonPushedFromPlan = filter.condition.children.collect {
      case e: AttributeReference => e.name
    }
    nonPushedFromPlan.toSet should be(nonPushedColumns.toSet)
  }

  def assertPushedPredicate(dataFrame: DataFrame, pushedPredicate: Filter*): DataFrame = debug(dataFrame) {
    val plan = dataFrame.queryExecution.sparkPlan
    val source = findDataSource(plan)
    withClue("The given df contains unexpected set of push down filters") {
      source.handledFilters.filterNot(_.isInstanceOf[IsNotNull]) shouldBe pushedPredicate.toSet
    }
  }
}
