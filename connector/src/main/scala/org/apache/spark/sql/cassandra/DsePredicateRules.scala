/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.{ColumnDef, TableDef}
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources.{EqualTo, Filter, In, IsNotNull}


/**
  *  A series of pushdown rules that only apply when connecting to Datastax Enterprise
  */
object DsePredicateRules extends CassandraPredicateRules {

  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {
    
    pushOnlySolrQuery(predicates, tableDef)
  }

  /**
    * When using a "solr_query = clause" we need to remove all other predicates to be pushed down.
    *
    * Example : SparkSQL : "SELECT * FROM ks.tab WHERE solr_query = data:pikachu and pkey = bob"
    *
    * In this example only the predicate solr_query can be handled in CQL. Passing pkey as well
    * will cause an exception.
    */
  def pushOnlySolrQuery(
    predicates: AnalyzedPredicates,
    tableDef: TableDef): AnalyzedPredicates = {

    val allPredicates = predicates.handledByCassandra ++ predicates.handledBySpark

    val solrQuery: Set[Filter] = allPredicates.collect {
      case EqualTo(column, value) if column == "solr_query" => EqualTo(column, value)
    }

    //Spark 2.0 + generates an IsNotNull when we do "= literal"
    val solrIsNotNull: Set[Filter] = allPredicates.collect {
      case IsNotNull(column) if column == "solr_query" => IsNotNull(column)
    }

    if (solrQuery.nonEmpty) {
      AnalyzedPredicates(solrQuery, allPredicates -- solrQuery -- solrIsNotNull)
    } else {
      predicates
    }
  }

}



