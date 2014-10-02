package com.datastax.spark.connector.util

object MagicalTypeTricks {

  trait DoesntHaveImplicit[A, B]
  implicit def doesntHaveImplicit[A, B] : A DoesntHaveImplicit B = null
  implicit def doesntHaveImplicitAmbiguity1[A, B](implicit ev: B) : A DoesntHaveImplicit B = null
  implicit def doesntHaveImplicitAmbiguity2[A, B](implicit ev: B) : A DoesntHaveImplicit B = null

  trait IsNotEqualTo[A, B]
  implicit def neq[A, B] : A IsNotEqualTo B = null
  implicit def neqAmbiguity1[A] : A IsNotEqualTo A = null
  implicit def neqAmbiguity2[A] : A IsNotEqualTo A = null

  trait IsNotSubclassOf[A, B]
  implicit def nsub[A, B] : A IsNotSubclassOf B = null
  implicit def nsubAmbiguity1[A, B >: A] : A IsNotSubclassOf B = null
  implicit def nsubAmbiguity2[A, B >: A] : A IsNotSubclassOf B = null


}
