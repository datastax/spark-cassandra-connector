/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.util

import java.time.{Duration => JavaDuration}
import java.util.concurrent.Callable
import java.util.function
import java.util.function.{Consumer, Predicate, Supplier}

import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.language.implicitConversions

object ScalaJavaUtil {

  implicit class JavaDurationWrapper(val duration: JavaDuration) extends AnyVal {
    def asScalaDuration: ScalaDuration = ScalaDuration.fromNanos(duration.toNanos).toCoarsest
  }

  implicit class ScalaDurationWrapper(val duration: ScalaDuration) extends AnyVal {
    def asJavaDuration: JavaDuration = JavaDuration.ofNanos(duration.toNanos)
  }

  implicit def asJavaCallable[T](f: () => T): Callable[T] = new Callable[T] {
    override def call(): T = f.apply()
  }

  implicit def asJavaPredicate[T](f: T => Boolean): Predicate[T] = new Predicate[T] {
    override def test(t: T): Boolean = f.apply(t)
  }

  implicit def asJavaConsumer[T](f: T => Unit): Consumer[T] = new Consumer[T] {
    override def accept(t: T): Unit = f(t)
  }

  implicit def asJavaSupplier[T](f: () => T): Supplier[T] = new Supplier[T] {
    override def get(): T = f()
  }

  implicit def asJavaFunction[T, R](f: T => R): function.Function[T, R] = new function.Function[T, R] {
    override def apply(t: T): R = f(t)
  }

  def asScalaFunction[T, R](f: java.util.function.Function[T, R]): T => R = x => f(x)
}
