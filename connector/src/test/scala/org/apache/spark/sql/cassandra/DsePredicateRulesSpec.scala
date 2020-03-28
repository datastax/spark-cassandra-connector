package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources._
import org.scalatest.{FlatSpec, Matchers}

class DsePredicateRulesSpec extends FlatSpec with Matchers {

  val fakeTableDef = TableDef(
    "fake",
    "fake",
    Seq.empty,
    Seq.empty,
    Seq.empty
  )

  "DSE Predicate Rules" should "un-push all other predicates if solr_query exists" in {
    val solrquery: Set[Filter] = Set(EqualTo("solr_query", "*:*"))
    val otherPreds: Set[Filter] = Set(GreaterThan("x", "4"), LessThan("y", "3"))
    val preds = AnalyzedPredicates(
      solrquery ++ otherPreds,
      Set.empty)

    val results = DsePredicateRules.apply(preds, fakeTableDef, new SparkConf())
    results should be (AnalyzedPredicates(solrquery, otherPreds))
  }

  "it" should "do nothing if solr_query is not present in a query" in {
    val otherPreds: Set[Filter] = Set(GreaterThan("x", "4"), LessThan("y", "3"))
    val preds = AnalyzedPredicates(
      otherPreds,
      Set.empty)

    val results = DsePredicateRules.apply(preds, fakeTableDef, new SparkConf())
    results should be (AnalyzedPredicates(otherPreds, Set.empty))
  }
}

