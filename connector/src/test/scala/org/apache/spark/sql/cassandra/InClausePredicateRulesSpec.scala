/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{IntType, TextType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.InClauseToFullTableScanConversionThreshold
import org.apache.spark.sql.cassandra.PredicateOps.FilterOps.columnName
import org.apache.spark.sql.sources._
import org.scalatest.{FlatSpec, Matchers}

class InClausePredicateRulesSpec extends FlatSpec with Matchers {

  val fakeTableDef = TableDef(
    "fake",
    "fake",
    partitionKey = Seq(
      ColumnDef("p1", PartitionKeyColumn, IntType),
      ColumnDef("p2", PartitionKeyColumn, IntType)),
    clusteringColumns = Seq(
      ColumnDef("c1", ClusteringColumn(0), IntType),
      ColumnDef("c2", ClusteringColumn(1), IntType)),
    regularColumns = Seq(
      ColumnDef("a", RegularColumn, IntType),
      ColumnDef("b", RegularColumn, TextType))
  )

  val conf = new SparkConf()

  private def cassandraPredicates(filters: Set[Filter]): AnalyzedPredicates =
    AnalyzedPredicates(filters, Set.empty)

  "InClausePredicateRules" should "leave partition and clustering filters if 'IN' filters are absent" in {
    val filters: Set[Filter] = Set(
      EqualTo("p1", "1"),
      EqualTo("p2", "1"),
      EqualTo("c1", "1"),
      EqualTo("c2", "1")
    )

    conf.set(InClauseToFullTableScanConversionThreshold.name, "1")

    val results = InClausePredicateRules.apply(cassandraPredicates(filters), fakeTableDef, conf)

    results should be(AnalyzedPredicates(filters, Set()))
  }

  it should "remove clustering filters if cartesian product of number of values is above threshold but partition values " +
    "cartesian number is below the threshold" in {
    val filters: Set[Filter] = Set(
      In("p1", Array(1, 2)),
      In("p2", Array(1, 2)),
      In("c1", Array(1, 2)),
      EqualTo("c2", 1)
    )

    conf.set(InClauseToFullTableScanConversionThreshold.name, "5")

    val results = InClausePredicateRules.apply(cassandraPredicates(filters), fakeTableDef, conf)

    results.handledByCassandra.size should be (3)
    results.handledByCassandra.forall(f => columnName(f) != "c1") should be(true)

    results.handledBySpark.size should be (1)
    columnName(results.handledBySpark.head) should be ("c1")
  }

  it should "remove partition and clustering filters if cartesian product of number of values is above threshold" in {
    val filters: Set[Filter] = Set(
      In("p1", Array(1, 2)),
      In("p2", Array(1, 2)),
      In("c1", Array(1, 2)),
      In("c2", Array(1, 2))
    )

    conf.set(InClauseToFullTableScanConversionThreshold.name, "1")

    val results = InClausePredicateRules.apply(cassandraPredicates(filters), fakeTableDef, conf)

    results should be(AnalyzedPredicates(Set(), filters))
  }

}

