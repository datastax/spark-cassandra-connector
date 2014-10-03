package com.datastax.spark.connector.util

object MagicalTypeTricks {

  trait DoesntHaveImplicit[A, B]
  implicit def doesntHaveImplicit[A, B]: A DoesntHaveImplicit B = null
  implicit def doesntHaveImplicitAmbiguity1[A, B](implicit ev: B): A DoesntHaveImplicit B = null
  implicit def doesntHaveImplicitAmbiguity2[A, B](implicit ev: B): A DoesntHaveImplicit B = null

  trait IsNotEqualTo[A, B]
  implicit def neq[A, B]: A IsNotEqualTo B = null
  implicit def neqAmbiguity1[A]: A IsNotEqualTo A = null
  implicit def neqAmbiguity2[A]: A IsNotEqualTo A = null

  trait IsNotSubclassOf[A, B]
  implicit def nsub[A, B]: A IsNotSubclassOf B = null
  implicit def nsubAmbiguity1[A, B >: A]: A IsNotSubclassOf B = null
  implicit def nsubAmbiguity2[A, B >: A]: A IsNotSubclassOf B = null

  type ¬[A] = A => Nothing
  type λ[A] = ¬[¬[A]]

  /**
   * Example of how disjunction can be used:
   * {{{
   * scala> import com.datastax.spark.connector.util.MagicalTypeTricks._
   * import com.datastax.spark.connector.util.MagicalTypeTricks._
   *
   * scala> def function[T](t: T)(implicit ev: (λ[T] <:< (Int ∪ String))) = { println(s"t = $t") }
   * function: [T](t: T)(implicit ev: <:<[(T => Nothing) => Nothing,Int => Nothing with String => Nothing => Nothing])Unit
   *
   * scala> function(5)
   * t = 5
   *
   * scala> function("five")
   * t = five
   *
   * scala> function(5d)
   * <console>:13: error: Cannot prove that (Double => Nothing) => Nothing <:< Int => Nothing with String => Nothing => Nothing.
   * function(5d)
   * ^
   * }}}
   *
   * Based on [[http://www.chuusai.com/2011/06/09/scala-union-types-curry-howard/ this article]].
   */
  type ∪[T, U] = ¬[¬[T] with ¬[U]]

}
