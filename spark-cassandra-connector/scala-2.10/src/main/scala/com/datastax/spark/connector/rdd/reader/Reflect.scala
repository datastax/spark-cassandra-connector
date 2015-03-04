package com.datastax.spark.connector.rdd.reader

import scala.reflect.runtime.universe._

private[reader] object Reflect {

  def constructor(tpe: Type): Symbol = tpe.declaration(nme.CONSTRUCTOR)

  def member(tpe: Type, name: String): Symbol = tpe.member(newTermName(name))

}
