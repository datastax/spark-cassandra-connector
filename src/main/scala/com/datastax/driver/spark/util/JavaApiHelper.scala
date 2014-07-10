package com.datastax.driver.spark.util

import scala.reflect.api._
import scala.reflect.runtime.universe._
import scala.reflect.api.{Mirror, TypeCreator}

/** A helper class to make it possible to access components written in Scala from Java code. */
object JavaApiHelper {

  /** Returns a [[TypeTag]] for the given class. */
  def getTypeTag[T](clazz: Class[T]): Universe#TypeTag[T] = {
    TypeTag.apply(runtimeMirror(Thread.currentThread().getContextClassLoader), new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type = {
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }

}
