package com.datastax.spark.connector.rdd.reader

import java.lang.reflect.Constructor

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/** Factory for creating objects of any type by invoking their primary constructor.
  * Unlike Java reflection Methods or Scala reflection Mirrors, this factory is serializable
  * and can be safely passed along with Spark tasks. */
class AnyObjectFactory[T : TypeTag] extends Serializable {

  @transient
  private val tpe = implicitly[TypeTag[T]].tpe

  val argCount: Int = {
    val ctorSymbol = tpe.declaration(nme.CONSTRUCTOR).asMethod
    ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType].params.size
  }

  @transient
  private val rm: RuntimeMirror =
    runtimeMirror(Thread.currentThread().getContextClassLoader)

  // This must be serialized:
  val javaClass: Class[T] =
    rm.runtimeClass(tpe).asInstanceOf[Class[T]]

  @transient
  private lazy val javaConstructor: Constructor[T] =
    javaClass.getConstructors()(0).asInstanceOf[Constructor[T]]

  private def isInnerClass =
    javaConstructor.getParameterTypes.length > argCount

  private def argOffset =
    if (isInnerClass) 1 else 0

  @transient
  private lazy val argBuffer = {
    val buffer = Array.ofDim[AnyRef](argOffset + argCount)
    if (isInnerClass) {
      val root = extractRoot(javaClass)
      val rootInstance = root.newInstance().asInstanceOf[AnyRef]
      buffer(0) = dive(rootInstance.asInstanceOf[AnyRef])
    }
    buffer
  }

  def newInstance(args: AnyRef*): T = {
    for (i <- 0 until argCount)
      argBuffer(i + argOffset) = args(i)
    javaConstructor.newInstance(argBuffer: _*)
  }

  private def extractRoot(c: Class[_]): Class[_] = {
    Try {
      c.getDeclaredField("$outer")
    } match {
      case Success(outer) => extractRoot(outer.getType)
      case Failure(ex: NoSuchFieldException) => c
      case Failure(ex) => throw ex;
    }
  }

  private def dive(instance: AnyRef): AnyRef = {
    Try {
      instance.getClass.getDeclaredField("$iw")
    } match {
      case Success(iw) =>
        iw.setAccessible(true)
        dive(iw.get(instance))
      case Failure(ex: NoSuchFieldException) => instance
      case Failure(ex) => throw ex;
    }
  }

}
