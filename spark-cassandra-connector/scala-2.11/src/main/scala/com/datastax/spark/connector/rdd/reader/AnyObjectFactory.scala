package com.datastax.spark.connector.rdd.reader

import scala.reflect.runtime.universe._

/** Factory for creating objects of any type by invoking their primary constructor.
  * Unlike Java reflection Methods or Scala reflection Mirrors, this factory is serializable
  * and can be safely passed along with Spark tasks. */
class AnyObjectFactory[T: TypeTag] extends AbstractObjectFactory[T] {

  @transient
  lazy val constructorDecl: Symbol = tpe.decl(termNames.CONSTRUCTOR)

}

