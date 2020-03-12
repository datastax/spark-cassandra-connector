/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.spark.connector.util

import scala.reflect.runtime.universe._

private[connector] object Reflect {

  def constructor(tpe: Type): Symbol = tpe.decl(termNames.CONSTRUCTOR)

  def member(tpe: Type, name: String): Symbol = tpe.member(TermName(name))

  def methodSymbol(tpe: Type): MethodSymbol = {
    val constructors = constructor(tpe).asTerm.alternatives.map(_.asMethod)
    val paramCount = constructors.map(_.paramLists.flatten.size).max
    constructors.filter(_.paramLists.flatten.size == paramCount) match {
      case List(onlyOne) => onlyOne
      case _             => throw new IllegalArgumentException(
        "Multiple constructors with the same number of parameters not allowed.")
    }
  }
}

