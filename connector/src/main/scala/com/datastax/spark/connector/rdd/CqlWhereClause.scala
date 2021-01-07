package com.datastax.spark.connector.rdd

/** Represents a logical conjunction of CQL predicates.
  * Each predicate can have placeholders denoted by '?' which get substituted by values from the `values` array.
  * The number of placeholders must match the size of the `values` array. */
case class CqlWhereClause(predicates: Seq[String], values: Seq[Any]) {

  assert(predicates.map(_.count(ch => ch == '?')).sum == values.size, "The number of placeholders is different than " +
    s"the number of values, this should never happen: $predicates, $values")

  /** Returns a conjunction of this clause and the given predicate. */
  def and(other: CqlWhereClause) =
    CqlWhereClause(predicates ++ other.predicates, values ++ other.values)

  override def toString: String = {
    val valuesIterator = values.iterator
    val predicatesWithValues = predicates.map { predicate =>
      val valuesCount = predicate.count(_ == '?')
      (predicate, (0 until valuesCount).map(_ => valuesIterator.next()))
    }
    predicatesWithValues.map { case (pred, values) =>
      val valuesString = if (values.size == 1) values.head else values.mkString("(", ", ", ")")
      s"[$pred, $valuesString]"
    }.mkString("[", ",", "]")
  }
}

object CqlWhereClause {

  /** Empty CQL WHERE clause selects all rows */
  val empty = new CqlWhereClause(Nil, Nil)
}


