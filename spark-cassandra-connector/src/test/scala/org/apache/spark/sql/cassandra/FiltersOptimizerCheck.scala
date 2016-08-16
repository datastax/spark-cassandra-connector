package org.apache.spark.sql.cassandra

import org.apache.spark.sql.sources._

import org.scalacheck._
import org.scalacheck.Prop.forAll
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, ShouldMatchers}

class FiltersOptimizerCheck extends FlatSpec with PropertyChecks with ShouldMatchers {

  // For testing purpose
  case object True extends Filter
  case object False extends Filter

  val genFullTree = for {
    size <- Gen.choose(0, 500)
    tree <- genTree(size)
  } yield tree

  def genTree(maxDepth: Int): Gen[Filter] = 
      if (maxDepth == 0) leaf else Gen.oneOf(leaf, genAnd(maxDepth), genOr(maxDepth), genNot(maxDepth))

  def genAnd(maxDepth: Int): Gen[Filter] = for {
      depthL <- Gen.choose(0, maxDepth - 1)
      depthR <- Gen.choose(0, maxDepth - 1)
      left <- genTree(depthL)
      right <- genTree(depthR)
  } yield And(left, right)

  def genOr(maxDepth: Int): Gen[Filter] = for {
      depthL <- Gen.choose(0, maxDepth - 1)
      depthR <- Gen.choose(0, maxDepth - 1)
      left <- genTree(depthL)
      right <- genTree(depthR)
  } yield Or(left, right)

  def genNot(maxDepth: Int): Gen[Filter] = for {
      depth <- Gen.choose(0, maxDepth - 1)
      expr <- genTree(depth)
  } yield Not(expr)

  def leaf: Gen[Filter] = Gen.oneOf(True, False)

  /**
   * Evaluate logical ADT
   **/
  private def eval(clause: Filter): Boolean = clause match {
    case And(left, right) => eval(left) && eval(right)
    case Or(left, right) => eval(left) || eval(right)
    case Not(predicate) => !eval(predicate)
    case True => true
    case False => false
  }

  "FiltersOptimizer" should "generate equivalent disjunction normal form for arbitrary logical statement" in {
    forAll(genFullTree){ expr =>
      val dnf = (FiltersOptimizer.toNNF andThen FiltersOptimizer.toDNF).apply(expr)
      assert(eval(dnf) == eval(expr))
    }
  }
  
}
