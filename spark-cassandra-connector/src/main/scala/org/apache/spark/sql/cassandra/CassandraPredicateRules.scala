package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.TableDef

case class AnalyzedPredicates[Predicate : PredicateOps](
  handledByCassandra: Set[Predicate],
  handledBySpark: Set[Predicate] )

trait CassandraPredicateRules{
  def applyRules[T](predicates: AnalyzedPredicates[T], tableDef: TableDef): AnalyzedPredicates[T]
}
