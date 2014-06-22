package com.datastax.driver.spark.rdd

case class CqlWhereClause(predicates: Seq[String], values: Seq[Any]) {

  def + (other: CqlWhereClause) =
    CqlWhereClause(predicates ++ other.predicates, values ++ other.values)

}

object CqlWhereClause {
  val empty = new CqlWhereClause(Nil, Nil)
}


