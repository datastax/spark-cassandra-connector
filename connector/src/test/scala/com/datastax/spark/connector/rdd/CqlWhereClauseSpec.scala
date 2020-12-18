package com.datastax.spark.connector.rdd

import org.scalatest.{FlatSpec, Matchers}

class CqlWhereClauseSpec extends FlatSpec with Matchers {

  it should "produce a string for each predicate" in {
    val clause = CqlWhereClause(Seq("x < ?", "y = ?"), Seq(1, "aaa"))

    clause.toString shouldBe "[[x < ?, 1],[y = ?, aaa]]"
  }

  it should "produce empty predicate string for empty predicate list" in {
    val clause = CqlWhereClause(Seq(), Seq())

    clause.toString shouldBe "[]"
  }

  it should "produce valid string for IN clause predicate" in {
    val clause = CqlWhereClause(Seq("x < ?", "z IN (?, ?)", "y IN (?, ?, ?)", "a = ?"), Seq(1, 2, 3, 4, 5, 6, 7))

    clause.toString shouldBe "[[x < ?, 1],[z IN (?, ?), (2, 3)],[y IN (?, ?, ?), (4, 5, 6)],[a = ?, 7]]"
  }

  it should "complain when the number of values doesn't match the number of placeholders '?'" in {
    intercept[AssertionError] {
      CqlWhereClause(Seq("x < ?"), Seq())
    }
  }
}
