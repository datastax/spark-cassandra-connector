package com.datastax.spark.connector.rdd.reader

import scala.reflect.runtime.universe._

private[reader] object Reflect {

  def constructor(tpe: Type): Symbol =
    tpe.decl(termNames.CONSTRUCTOR)

  def method(tpe: Type, name: String): MethodSymbol =
    tpe.decl(TermName(name)).asMethod

}

