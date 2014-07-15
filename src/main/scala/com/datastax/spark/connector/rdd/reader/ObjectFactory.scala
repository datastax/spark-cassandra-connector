package com.datastax.spark.connector.rdd.reader

import java.lang.reflect.Constructor

import org.apache.spark.Logging

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

trait ObjectFactory[T] extends Serializable {
  def newInstance(argValues: AnyRef*): T

  def argCount: Int

  def javaClass: Class[T]

  def constructorParamTypes: Array[Type]
}

/** Factory for creating objects of any type by invoking their primary constructor.
  * Unlike Java reflection Methods or Scala reflection Mirrors, this factory is serializable
  * and can be safely passed along with Spark tasks. */
class AnyObjectFactory[T : TypeTag] extends ObjectFactory[T] {

  @transient
  private val tpe = implicitly[TypeTag[T]].tpe

  override val argCount: Int = {
    val ctorSymbol = tpe.declaration(nme.CONSTRUCTOR).asMethod
    ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType].params.size
  }

  @transient
  private val rm: RuntimeMirror =
    runtimeMirror(Thread.currentThread().getContextClassLoader)

  // This must be serialized:
  override val javaClass: Class[T] =
    rm.runtimeClass(tpe).asInstanceOf[Class[T]]

  @transient
  private lazy val javaConstructor: Constructor[T] =
    javaClass.getConstructors()(0).asInstanceOf[Constructor[T]]

  @transient
  override val constructorParamTypes: Array[Type] = {
    val ctorSymbol = tpe.declaration(nme.CONSTRUCTOR).asMethod
    val ctorType = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    ctorType.params.map(_.asTerm.typeSignature).toArray
  }

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

  override def newInstance(args: AnyRef*): T = {
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


/** Factory for creating Java-bean style objects, which may include several constructors.
  * Unlike Java reflection Methods or Scala reflection Mirrors, this factory is serializable
  * and can be safely passed along with Spark tasks. */
class JavaObjectFactory[T : TypeTag] extends ObjectFactory[T] with Logging {

  @transient
  private val tpe = implicitly[TypeTag[T]].tpe

  @transient
  private val rm: RuntimeMirror =
    runtimeMirror(Thread.currentThread().getContextClassLoader)

  // This must be serialized:
  override val javaClass: Class[T] =
    rm.runtimeClass(tpe).asInstanceOf[Class[T]]

  // we want this test to be performed eagerly on the client side
  if (javaClass.isMemberClass)
    throw new IllegalArgumentException(s"Class $javaClass is a member class. Non-static inner classes are not supported yet.")

  @transient
  private lazy val javaConstructor: Constructor[T] = javaClass.getConstructor()

  // we want this test to be performed eagerly on the client side
  try {
    javaConstructor
  } catch {
    case ex: Exception => throw new NoSuchMethodException(s"Class $javaClass does not have a no-args constructor.")
  }

  override val argCount = 0

  @transient
  override val constructorParamTypes: Array[Type] = Array()

  override def newInstance(args: AnyRef*): T = {
    javaConstructor.newInstance()
  }

}
