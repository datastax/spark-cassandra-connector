package org.apache.spark.sql.cassandra

import org.apache.spark.sql.sources._


/**
  * Optimizer will try to transform pushdown filter into `sum of products`.
  * So that the filter like
  * '(field1 < 3 OR field1 > 7) AND (field2 = 'val1' OR field2 = 'val2')'
  * will become equivalent
  * 'field1 < 3 AND field2 = "val1" OR field1 < 3 AND field2 = "val2" OR
  * field1 > 7 AND field2 = "val1" OR field1 > 7 AND field2 = "val2"'
  */
class FiltersOptimizer(filters: Array[Filter]) {

  private val fullFilterAst =
    if (filters.nonEmpty) Some(filters.reduce((left, right) => And(left, right))) else None

  private def dist(predL: Filter, predR: Filter): Filter = (predL, predR) match {
    case (Or(l, r), p) => Or(dist(l, p), dist(r, p))
    case (p, Or(l, r)) => Or(dist(p, l), dist(p, r))
    case (l, r) => And(l, r)
  }

  /** The 'toNNF' function converts expressions to negation normal form. This
    * function is total: it's defined for all expressions, not just those which
    * only use negation, conjunction and disjunction, although all expressions in
    * negation normal form do in fact only use those connectives.
    *
    * Then de Morgan's laws are applied to convert negated
    * conjunctions and disjunctions into the conjunction or disjunction of the
    * negation of their conjuncts: ¬(φ ∧ ψ) is converted to (¬φ ∨ ¬ψ)
    * while ¬(φ ∨ ψ) becomes (¬φ ∧ ¬ψ).
    */
  private val toNNF: Filter => Filter = {
    case a@(EqualTo(_, _) | EqualNullSafe(_, _) | GreaterThan(_, _) |
            GreaterThanOrEqual(_, _) | LessThan(_, _) | LessThanOrEqual(_, _) |
            In(_, _) | IsNull(_) | IsNotNull(_) |
            StringStartsWith(_, _) | StringEndsWith(_, _) | StringContains(_, _)) => a
    case a@Not(EqualTo(_, _) | EqualNullSafe(_, _) | In(_, _) |
               StringStartsWith(_, _) | StringEndsWith(_, _) | StringContains(_, _)) => a
    case Not(GreaterThan(a, v)) => LessThanOrEqual(a, v)
    case Not(LessThanOrEqual(a, v)) => GreaterThan(a, v)
    case Not(LessThan(a, v)) => GreaterThanOrEqual(a, v)
    case Not(GreaterThanOrEqual(a, v)) => LessThan(a, v)
    case Not(IsNull(a)) => IsNotNull(a)
    case Not(IsNotNull(a)) => IsNull(a)
    case Not(Not(p)) => p
    case And(l, r) => And(toNNF(l), toNNF(r))
    case Not(And(l, r)) => toNNF(Or(Not(l), Not(r)))
    case Or(l, r) => Or(toNNF(l), toNNF(r))
    case Not(Or(l, r)) => toNNF(And(Not(l), Not(r)))
  }

  /** The 'toDNF' function converts expressions to disjunctive normal form: a
    * disjunction of clauses, where a clause is a conjunction of literals
    * (variables and negated variables).
    *
    * The conversion is carried out by first converting the expression into
    * negation normal form, and then applying the distributive law.
    */
  private val toDNF: Filter => Filter = {
    case And(l, r) => dist(toDNF(l), toDNF(r))
    case Or(l, r) => Or(toDNF(l), toDNF(r))
    case p => p
  }

  /**
    * Traverse over disjunctive parts of AST
    */
  private val traverse: Filter => List[Filter] = {
    case Or(l, r) => traverse(l) ++ traverse(r)
    case a => a :: Nil
  }

  private val andToArray: Filter => Array[Filter] = {
    case And(l, r) => andToArray(l) ++ andToArray(r)
    case a => Array(a)
  }

  private val groupAnd: List[Filter] => List[Array[Filter]] = _.map(andToArray)

  def build(): List[Array[Filter]] = fullFilterAst match {
    case Some(ast) => (toNNF andThen toDNF andThen traverse andThen groupAnd).apply(ast)
    case None => List.empty
  }

}
