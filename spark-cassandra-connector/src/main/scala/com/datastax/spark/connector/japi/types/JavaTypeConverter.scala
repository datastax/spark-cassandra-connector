package com.datastax.spark.connector.japi.types

import akka.japi.JavaPartialFunction
import com.datastax.spark.connector.types.NullableTypeConverter

import scala.reflect.runtime.universe._

class JavaTypeConverter[T <: AnyRef](typeTag: TypeTag[T], convertFunction: JavaPartialFunction[Any, T])
  extends NullableTypeConverter[T] {

  override def targetTypeTag: TypeTag[T] = typeTag

  override def convertPF: PartialFunction[Any, T] = convertFunction

  def noMatch() = JavaPartialFunction.noMatch()
}
