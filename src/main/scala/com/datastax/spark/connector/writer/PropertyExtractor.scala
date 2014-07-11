package com.datastax.spark.connector.writer

import java.lang.reflect.Method

import scala.util.Try

/** Extracts values from fields of an object. */
class PropertyExtractor[T](val cls: Class[T], val propertyNames: Seq[String]) extends Serializable {

  private def getter(name: String) =
    cls.getMethod(name)

  @transient
  private lazy val methods: Array[Method] =
    propertyNames.map(getter).toArray

  def extract(obj: T): Array[AnyRef] =
    extract(obj, Array.ofDim(methods.length))

  def extract(obj: T, target: Array[AnyRef]): Array[AnyRef] = {
    for (i <- 0 until methods.length)
      target(i) = methods(i).invoke(obj)
    target
  }

}

object PropertyExtractor {

  def availablePropertyNames(cls: Class[_], requestedPropertyNames: Seq[String]): Seq[String] =
    requestedPropertyNames.filter(name => Try(cls.getMethod(name)).isSuccess)

}
